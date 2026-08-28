//! Post-startup connection state machine, generic over the byte stream so the
//! same code runs plaintext and TLS sessions.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_trace_context::TraceCarrier;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    engine::{
        BoundParam, CloseTarget, CopyInResponse, CopyOutStream, Engine, ExecuteOutcome,
        Notification, QueryResult, ReportedParameter, ResultPage, ResultSink, Session,
        SimpleQueryStop, TxStatus,
    },
    error::{PgError, Severity, sqlstate},
    messages::{
        backend,
        frontend::{self, FrontendMessage},
    },
    server::{SessionActivity, SessionCancel},
    telemetry::{self, IngressTracePolicy, StatementProtocol},
};

#[derive(Debug, Clone)]
pub enum AuthMode {
    Trust,
    /// SCRAM-SHA-256 against stored verifiers, with no plaintext at rest. A
    /// server mock secret derives a deterministic fake verifier for unknown
    /// users, so the message sequence and timing match a real user. This
    /// closes the username-enumeration oracle (RFC 5802 mock authentication).
    ScramSha256 {
        verifiers: std::collections::HashMap<String, crate::scram::ScramVerifier>,
        mock_secret: [u8; 32],
        mock_iterations: u32,
    },
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub auth: AuthMode,
    pub max_message_len: usize,
    /// The server-wide floor of the startup `ParameterStatus` burst; see
    /// [`default_server_params`]. Clients parse `server_version` and depend on
    /// `client_encoding=UTF8`. A session announces these unless its own
    /// startup packet or its engine has something truer to say.
    pub server_params: Vec<(String, String)>,
    /// How much of a client-supplied W3C trace context statements on this
    /// connection may inherit. See [`IngressTracePolicy`].
    pub ingress_trace: IngressTracePolicy,
}

impl SessionConfig {
    #[must_use]
    pub fn trust() -> Self {
        Self {
            auth: AuthMode::Trust,
            max_message_len: crate::messages::frontend::MAX_MESSAGE_LEN,
            server_params: default_server_params(),
            ingress_trace: IngressTracePolicy::default(),
        }
    }
}

/// The static floor of the startup `ParameterStatus` burst.
///
/// `PostgreSQL` marks fifteen GUCs `GUC_REPORT` in `guc_tables.c` and announces
/// every one of them at startup; a live 18.4 backend sends exactly those
/// fifteen. Twelve are here. The three that are not are the three no gres
/// session can answer honestly:
///
/// * `is_superuser` and `session_authorization` are authorization state the
///   engine owns, and the engine does not yet learn who connected — the wire
///   layer never passes `user` down, so a gres session's user is always
///   `public`. Announcing the startup packet's `user` would contradict
///   `SELECT session_user` on the very same connection.
/// * `scram_iterations` sets the iteration count used when *generating* a SCRAM
///   secret. Gres never generates one; its verifiers arrive already built. The
///   parameter is not a GUC gres has, so `SHOW scram_iterations` fails, and a
///   `ParameterStatus` for it would be a claim about a capability that is not
///   there.
///
/// Every parameter that is here is one gres can also answer through `SHOW`,
/// which is the test each of them had to pass.
///
/// The values are the server's, not the session's: a client that set one of
/// them in its startup packet is answered from
/// [`Session::reported_parameters`] instead, and for
/// [`VERBATIM_REPORTED_PARAMS`] from the startup packet itself.
#[must_use]
pub fn default_server_params() -> Vec<(String, String)> {
    [
        // `PQserverVersion` parses this, so it has to name the same release as
        // `version()` and `server_version_num`, which are both 18.4.
        ("server_version", "18.4"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("application_name", ""),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("search_path", "\"$user\", public"),
        ("default_transaction_read_only", "off"),
        ("in_hot_standby", "off"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Reported parameters an engine stores exactly as the startup packet spelled
/// them, so the wire layer can announce the client's own value without having
/// to ask what the engine made of it.
///
/// The rest normalise, and only the engine knows the canonical form: measured
/// against `PostgreSQL` 18.4, a startup `DateStyle=Postgres` is announced as
/// `Postgres, MDY`, `TimeZone=utc` as `UTC`, and `IntervalStyle=SQL_STANDARD`
/// as `sql_standard`. Those come from [`Session::reported_parameters`].
const VERBATIM_REPORTED_PARAMS: [&str; 2] = ["application_name", "search_path"];

/// A connection's `ParameterStatus` bookkeeping: the engine's change stream,
/// and the value last announced for each reported parameter.
///
/// The last-announced map is what makes a change *report* rather than an echo.
/// `PostgreSQL` keeps a `reported_value` per GUC and stays silent when the
/// current value already matches it, so rolling a `SET LOCAL` back to the value
/// the client was already told produces no message at all. Holding the same
/// map here means an engine can push unconditionally on every assignment and
/// still put exactly `PostgreSQL`'s messages on the wire.
#[derive(Debug, Default)]
struct ParameterReporter {
    changes: Option<mpsc::Receiver<ReportedParameter>>,
    /// Keyed by the lowercased name, because GUC names are case-insensitive and
    /// an engine may not spell `DateStyle` the way the startup burst did. The
    /// stored name is the spelling this connection has already used, which is
    /// the one it keeps using.
    announced: std::collections::HashMap<String, (String, String)>,
}

impl ParameterReporter {
    /// Announce a parameter and remember it as this connection's current value.
    fn announce(&mut self, out: &mut BytesMut, name: &str, value: &str) {
        let key = name.to_ascii_lowercase();
        let name = match self.announced.get(&key) {
            Some((announced_name, announced_value)) => {
                if announced_value == value {
                    return;
                }
                announced_name.clone()
            }
            None => name.to_string(),
        };
        backend::parameter_status(out, &name, value);
        self.announced.insert(key, (name, value.to_string()));
    }

    /// Drain the engine's queue and write the `ParameterStatus` messages the
    /// round owes, collapsing repeats of one parameter to its final value.
    ///
    /// Order is the order each parameter first moved. A live 18.4 backend emits
    /// the reverse, because `ReportChangedGUCOptions` walks a list it builds by
    /// prepending; no client can tell, since the messages name themselves.
    fn flush(&mut self, out: &mut BytesMut) {
        let Some(changes) = self.changes.as_mut() else {
            return;
        };
        let mut round: Vec<ReportedParameter> = Vec::new();
        while let Ok(change) = changes.try_recv() {
            match round
                .iter_mut()
                .find(|seen| seen.name.eq_ignore_ascii_case(&change.name))
            {
                Some(seen) => seen.value = change.value,
                None => round.push(change),
            }
        }
        for change in round {
            self.announce(out, &change.name, &change.value);
        }
    }
}

// ── Extended-query state ────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct ExtendedState {
    /// True after an error in the extended phase: skip messages until Sync.
    failed: bool,
    /// Client trace context read from the sqlcommenter tag on the most recent
    /// `Parse`, which is the only extended-protocol message carrying SQL.
    ///
    /// The session clears this at `Sync`, together with
    /// [`ExtendedState::failed`], and that lifetime is the point. An *unnamed*
    /// prepared statement is a one-shot pipelined batch, which is what every
    /// ORM emits, so its Parse-time trace is exactly the trace its `Execute`
    /// belongs to. A *named* statement that survives a `Sync` is genuinely
    /// reused, and the trace it was prepared under is stale by the time it
    /// runs again.
    trace: TraceCarrier,
}

/// The trace context an `Execute`-raised `gres.statement` span hangs from.
///
/// `Bind` and `Execute` carry no SQL, so there is no sqlcommenter tag to read
/// at execution time. The precedence is:
///
/// 1. a `crabka.traceparent` GUC, the only genuinely per-execution channel, and
///    the one an application can set once per request. A read of it needs an
///    engine-side seam that lands with the pgexec statement tier. **This
///    function does not consult it yet** and is the single place that changes
///    when it does.
/// 2. the carrier captured from the `Parse` that prepared the statement, held on
///    [`ExtendedState`] until the next `Sync`.
/// 3. nothing, which leaves the statement span a trace root.
fn resolve_execute_parent(ext: &ExtendedState) -> &TraceCarrier {
    &ext.trace
}

#[derive(Debug)]
struct CopyInState {
    target: CopyInTarget,
    chunks: Vec<Bytes>,
}

/// How a COPY FROM STDIN was started, which decides how it completes and
/// whether the wire layer owes a `ReadyForQuery` after `CommandComplete`
/// (simple protocol) or must wait for the client's Sync (extended protocol).
#[derive(Debug)]
enum CopyInTarget {
    /// Simple-protocol Query whose whole text is the copy: complete with
    /// [`Session::copy_in`] and the SQL text, and owe nothing after it.
    Statement { sql: String },
    /// A copy the engine stopped at part-way through a multi-statement
    /// simple-protocol Query: complete it the same way, then resume the query
    /// string at the statement after `statement_index`. Only the resumption
    /// finishes the Query, so the `ReadyForQuery` is owed there, not here.
    Batched { sql: String, statement_index: usize },
    /// Extended-protocol Execute: complete with [`Session::copy_in_portal`].
    Portal { name: String },
}

fn resolve_param_formats(requested: &[i16], nparams: usize) -> Result<Vec<i16>, PgError> {
    let validate = |code: i16| -> Result<i16, PgError> {
        if code == 0 || code == 1 {
            Ok(code)
        } else {
            Err(PgError::protocol(format!(
                "invalid parameter format code {code}"
            )))
        }
    };
    match requested.len() {
        0 => Ok(vec![0; nparams]),
        1 => Ok(vec![validate(requested[0])?; nparams]),
        n if n == nparams => requested.iter().map(|&c| validate(c)).collect(),
        n => Err(PgError::protocol(format!(
            "bind message has {n} parameter formats but {nparams} parameters"
        ))),
    }
}

fn count_positional_parameters(sql: &str) -> Result<usize, PgError> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut max_parameter = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_single_quoted_string(bytes, index)?,
            b'"' => index = skip_double_quoted_identifier(bytes, index)?,
            b'-' if bytes.get(index + 1) == Some(&b'-') => index = skip_line_comment(bytes, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index)?;
            }
            b'$' => {
                if let Some((next_index, parameter)) = read_parameter_number(bytes, index) {
                    max_parameter = max_parameter.max(parameter);
                    index = next_index;
                } else {
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }

    Ok(max_parameter)
}

fn skip_single_quoted_string(bytes: &[u8], mut index: usize) -> Result<usize, PgError> {
    index += 1;
    while index < bytes.len() {
        if bytes[index] != b'\'' {
            index += 1;
            continue;
        }
        if bytes.get(index + 1) == Some(&b'\'') {
            index += 2;
            continue;
        }
        return Ok(index + 1);
    }
    Err(PgError::protocol(
        "unterminated string literal in parse message",
    ))
}

fn skip_double_quoted_identifier(bytes: &[u8], mut index: usize) -> Result<usize, PgError> {
    index += 1;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        if bytes.get(index + 1) == Some(&b'"') {
            index += 2;
            continue;
        }
        return Ok(index + 1);
    }
    Err(PgError::protocol(
        "unterminated quoted identifier in parse message",
    ))
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    index += 2;
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> Result<usize, PgError> {
    index += 2;
    while index + 1 < bytes.len() {
        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            return Ok(index + 2);
        }
        index += 1;
    }
    Err(PgError::protocol(
        "unterminated block comment in parse message",
    ))
}

fn read_parameter_number(bytes: &[u8], index: usize) -> Option<(usize, usize)> {
    let mut end = index + 1;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == index + 1 {
        return None;
    }
    let number = std::str::from_utf8(&bytes[index + 1..end])
        .ok()?
        .parse::<usize>()
        .ok()?;
    Some((end, number))
}

fn fail_extended(ext: &mut ExtendedState, out: &mut BytesMut, e: &PgError) {
    ext.failed = true;
    backend::error_response(out, e);
}

/// Prepare a statement and return the client trace context its SQL carried, so
/// the matching `Execute` can join the same trace.
///
/// `Parse` earns a span of its own because it is not always local. For a
/// sharded table the gateway forwards the prepare to the range owner, a real
/// network hop that is otherwise invisible.
async fn handle_parse<Sess: Session>(
    session: &mut Sess,
    name: String,
    sql: String,
    param_types: Vec<u32>,
    policy: IngressTracePolicy,
    out: &mut BytesMut,
) -> Result<TraceCarrier, PgError> {
    let span = telemetry::parse_span(&name);
    let carrier = telemetry::ingress_from_sql(policy, &sql, &span);
    let prepared: Result<(), PgError> = async {
        count_positional_parameters(&sql)?;
        session.parse(&name, &sql, &param_types).await?;
        Ok(())
    }
    .instrument(span.clone())
    .await;
    if let Err(error) = &prepared {
        telemetry::record_error(&span, error);
    }
    prepared?;
    backend::parse_complete(out);
    Ok(carrier)
}

async fn handle_bind<Sess: Session>(
    session: &mut Sess,
    portal: String,
    statement: &str,
    param_formats: &[i16],
    params: Vec<Option<Bytes>>,
    result_formats: &[i16],
    out: &mut BytesMut,
) -> Result<(), PgError> {
    let span = telemetry::bind_span(&portal, statement);
    let bound: Result<(), PgError> = async {
        let param_formats = resolve_param_formats(param_formats, params.len())?;
        let params = params
            .into_iter()
            .zip(param_formats)
            .map(|(value, format)| BoundParam {
                type_oid: None,
                format,
                value,
            })
            .collect::<Vec<_>>();
        session
            .bind(&portal, statement, &params, result_formats)
            .await?;
        Ok(())
    }
    .instrument(span.clone())
    .await;
    if let Err(error) = &bound {
        telemetry::record_error(&span, error);
    }
    bound?;
    backend::bind_complete(out);
    Ok(())
}

async fn handle_describe<Sess: Session>(
    session: &mut Sess,
    kind: u8,
    name: &str,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    let span = telemetry::describe_span(kind, name);
    let described: Result<(), PgError> = async {
        match kind {
            b'S' => {
                let description = session.describe_statement(name).await?;
                backend::parameter_description(out, &description.parameter_types);
                if description.fields.is_empty() {
                    backend::no_data(out);
                } else {
                    backend::row_description(out, &description.fields);
                }
            }
            b'P' => {
                let description = session.describe_portal(name).await?;
                if description.fields.is_empty() {
                    backend::no_data(out);
                } else {
                    // Describe(portal) reports the formats the portal will use.
                    backend::row_description(out, &description.fields);
                }
            }
            other => {
                return Err(PgError::protocol(format!(
                    "invalid describe kind {:?}",
                    other as char
                )));
            }
        }
        Ok(())
    }
    .instrument(span.clone())
    .await;
    if let Err(error) = &described {
        telemetry::record_error(&span, error);
    }
    described
}

/// Returns `Some(CopyInState)` when the portal is a COPY FROM STDIN. This
/// function has then written the `CopyInResponse`, and the caller must enter
/// copy-in mode.
async fn handle_execute<Sess: Session>(
    session: &mut Sess,
    portal_name: &str,
    max_rows: i32,
    token: CancellationToken,
    notices: Option<&mut mpsc::Receiver<PgError>>,
    out: &mut BytesMut,
) -> Result<Option<CopyInState>, PgError> {
    if max_rows < 0 {
        return Err(PgError::protocol(format!(
            "execute message has negative max rows: {max_rows}"
        )));
    }

    let outcome = tokio::select! {
        // biased + cancellation-first: a cancel that arrived before execution
        // (the pending flag from the extended-batch window) must win even
        // against an engine future that is ready on its first poll.
        biased;
        () = token.cancelled() => None,
        r = session.execute(portal_name, max_rows.cast_unsigned()) => Some(r?),
    };
    let Some(outcome) = outcome else {
        session.cancel_current_query().await;
        return Err(query_canceled());
    };
    write_notices(out, notices);
    if let ExecuteOutcome::CopyIn { response } = outcome {
        backend::copy_in_response(out, response.overall_format, &response.column_formats);
        return Ok(Some(CopyInState {
            target: CopyInTarget::Portal {
                name: portal_name.to_string(),
            },
            chunks: Vec::new(),
        }));
    }
    if let ExecuteOutcome::Rows { rows, .. } = &outcome {
        // One `Execute` yields at most one batch, so this *is* the caller's page
        // loop — the current span is the `gres.statement` the call was
        // instrumented with.
        telemetry::record_statement_rows(&tracing::Span::current(), rows.len(), 1);
    }
    encode_execute_outcome(out, outcome)?;
    Ok(None)
}

/// What a simple-protocol Query turned out to be, once the engine has been
/// asked whether it is a COPY the wire layer must drive itself.
enum SimpleCopy {
    /// COPY FROM STDIN: the connection enters copy-in mode.
    In(CopyInResponse),
    /// COPY TO STDOUT, already run to completion by the engine.
    Out(CopyOutStream),
    /// Ordinary SQL, to be executed through [`Session::simple_query_into`].
    None,
}

/// Ask the engine whether `sql` is a COPY, under the same cancellation
/// discipline as any other engine call: a `CancelRequest` that arrives while
/// the probe is in flight drops the future and reports `57014`.
async fn begin_simple_copy<Sess: Session>(
    session: &mut Sess,
    sql: &str,
    cancel: &SessionCancel,
) -> Result<SimpleCopy, PgError> {
    let token = cancel.begin_query();
    let started = tokio::select! {
        // biased + cancellation-first; see handle_execute.
        biased;
        () = token.cancelled() => None,
        r = session.begin_copy_in(sql) => Some(r),
    };
    let Some(started) = started else {
        session.cancel_current_query().await;
        return Err(query_canceled());
    };
    if let Some(response) = started? {
        return Ok(SimpleCopy::In(response));
    }

    let token = cancel.begin_query();
    let started = tokio::select! {
        biased;
        () = token.cancelled() => None,
        r = session.begin_copy_out(sql) => Some(r),
    };
    let Some(started) = started else {
        session.cancel_current_query().await;
        return Err(query_canceled());
    };
    Ok(started?.map_or(SimpleCopy::None, SimpleCopy::Out))
}

fn query_canceled() -> PgError {
    PgError::error(
        sqlstate::QUERY_CANCELED,
        "canceling statement due to user request",
    )
}

/// How a run of a simple-query string ended, and therefore what the connection
/// loop owes the client next.
enum BatchOutcome {
    /// Every remaining statement ran. `ReadyForQuery` is owed.
    Done,
    /// A `COPY … FROM STDIN` was reached at this index and its
    /// `CopyInResponse` is written. Nothing else is owed until the copy
    /// finishes and the string resumes.
    CopyIn { statement_index: usize },
    /// A diagnostic is written. `ReadyForQuery` is owed.
    Failed,
    /// A fatal diagnostic is written; the connection is over.
    Fatal(PgError),
}

/// Run `sql` from `from_statement` on, writing its results into `out`.
///
/// Everything this writes is buffered — the caller flushes, because only the
/// caller knows whether a `ReadyForQuery` belongs on the end of the same write.
async fn run_simple_batch<Sess: Session>(
    session: &mut Sess,
    sql: &str,
    from_statement: usize,
    cancel: &SessionCancel,
    statement_span: &tracing::Span,
    notices: &mut Option<mpsc::Receiver<PgError>>,
    out: &mut BytesMut,
) -> BatchOutcome {
    let token = cancel.begin_query();
    let mut sink = WireResultSink {
        out,
        notices: notices.as_mut(),
        rows: 0,
        pages: 0,
    };
    let outcome = tokio::select! {
        // biased + cancellation-first; see handle_execute.
        biased;
        () = token.cancelled() => None,
        r = session
            .simple_query_batch_into(sql, from_statement, 1024, &mut sink)
            .instrument(statement_span.clone()) => Some(r),
    };
    // Read off the sink's running totals before it is dropped: the pages
    // themselves get no spans, only this one summary.
    let (rows, pages) = (sink.rows, sink.pages);
    let outcome = if let Some(outcome) = outcome {
        outcome
    } else {
        session.cancel_current_query().await;
        Err(query_canceled())
    };
    telemetry::record_statement_rows(statement_span, rows, pages);
    match outcome {
        Ok(SimpleQueryStop::Done) => BatchOutcome::Done,
        Ok(SimpleQueryStop::CopyIn {
            statement_index,
            response,
        }) => {
            write_notices(out, notices.as_mut());
            backend::copy_in_response(out, response.overall_format, &response.column_formats);
            BatchOutcome::CopyIn { statement_index }
        }
        Err(e) => {
            telemetry::record_statement_error(statement_span, &e);
            write_notices(out, notices.as_mut());
            backend::error_response(out, &e);
            if e.severity == Severity::Fatal {
                BatchOutcome::Fatal(e)
            } else {
                BatchOutcome::Failed
            }
        }
    }
}

fn encode_execute_outcome(out: &mut BytesMut, outcome: ExecuteOutcome) -> Result<(), PgError> {
    match outcome {
        ExecuteOutcome::Rows { rows, completion } => {
            for row in &rows {
                backend::data_row(out, row);
            }
            if let Some(tag) = completion {
                backend::command_complete(out, &tag);
            } else {
                backend::portal_suspended(out);
            }
        }
        ExecuteOutcome::CommandComplete { tag } => backend::command_complete(out, &tag),
        ExecuteOutcome::EmptyQuery => backend::empty_query_response(out),
        ExecuteOutcome::CopyOut { stream } => write_copy_out(out, &stream),
        // CopyIn is intercepted by `handle_execute`, which owns the copy-in
        // state machine, so it never reaches the encoder.
        ExecuteOutcome::CopyIn { .. } | ExecuteOutcome::Notification { .. } => {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "execute outcome is reserved for a future protocol extension",
            ));
        }
    }
    Ok(())
}

/// Write a whole COPY TO STDOUT exchange: `CopyOutResponse`, one `CopyData` per
/// row, `CopyDone`, then the `CommandComplete` that ends the command.
///
/// The caller owes whatever `ReadyForQuery` the protocol requires — the simple
/// protocol one immediately, the extended protocol one at the client's Sync.
fn write_copy_out(out: &mut BytesMut, copy: &CopyOutStream) {
    backend::copy_out_response(
        out,
        copy.response.overall_format,
        &copy.response.column_formats,
    );
    for row in &copy.rows {
        backend::copy_data(out, row);
    }
    backend::copy_done(out);
    backend::command_complete(out, &copy.tag);
}

// ── Authentication helpers ──────────────────────────────────────────────────

/// Runs the authentication exchange. Returns Ok(false) if the client failed
/// authentication. The error is then already written to the stream.
async fn authenticate<S>(
    stream: &mut S,
    startup_params: &[(String, String)],
    config: &SessionConfig,
    out: &mut BytesMut,
    inbuf: &mut BytesMut,
) -> std::io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match &config.auth {
        AuthMode::Trust => {
            backend::authentication_ok(out);
            Ok(true)
        }
        AuthMode::ScramSha256 {
            verifiers,
            mock_secret,
            mock_iterations,
        } => {
            let user = startup_params
                .iter()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            let verifier = match verifiers.get(user) {
                Some(v) => v.clone(),
                None => crate::scram::ScramVerifier::mock_with_iterations(
                    mock_secret,
                    user,
                    *mock_iterations,
                ),
            };

            backend::authentication_sasl(out, &["SCRAM-SHA-256"]);
            stream.write_all(out).await?;
            out.clear();

            let Some(mut body) = read_password(stream, inbuf, config.max_message_len).await? else {
                return Ok(false);
            };
            let mechanism = frontend::get_cstr(&mut body).map_err(|_| bad_proto())?;
            if mechanism != "SCRAM-SHA-256" {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let len = frontend::get_i32(&mut body).map_err(|_| bad_proto())?;
            if len < 0 {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            }
            let client_first = body;

            let mut scram = crate::scram::ScramServer::from_verifier(verifier, server_nonce());
            let Ok(server_first) = scram.handle_client_first(&client_first) else {
                return send_auth_failure(stream, out, user).await.map(|()| false);
            };
            backend::authentication_sasl_continue(out, &server_first);
            stream.write_all(out).await?;
            out.clear();

            let Some(client_final) = read_password(stream, inbuf, config.max_message_len).await?
            else {
                return Ok(false);
            };
            match scram.handle_client_final(&client_final) {
                Ok(server_final) => {
                    backend::authentication_sasl_final(out, &server_final);
                    backend::authentication_ok(out);
                    Ok(true)
                }
                Err(_) => send_auth_failure(stream, out, user).await.map(|()| false),
            }
        }
    }
}

async fn send_auth_failure<S>(stream: &mut S, out: &mut BytesMut, user: &str) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let e = PgError::fatal(
        sqlstate::INVALID_PASSWORD,
        format!("password authentication failed for user \"{user}\""),
    );
    backend::error_response(out, &e);
    stream.write_all(out).await?;
    out.clear();
    Ok(())
}

fn bad_proto() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed SASL message")
}

fn server_nonce() -> String {
    use rand::{RngExt, distr::Alphanumeric};
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

/// Reads the next frontend message, which must be Password ('p'), and returns
/// its body.
async fn read_password<S>(
    stream: &mut S,
    inbuf: &mut BytesMut,
    max_message_len: usize,
) -> std::io::Result<Option<Bytes>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        match frontend::decode_message_with_max_len(inbuf, max_message_len) {
            Ok(Some(FrontendMessage::Password(body))) => return Ok(Some(body)),
            // Anything else mid-auth: give up.
            Ok(Some(_)) | Err(_) => return Ok(None),
            Ok(None) => {
                if stream.read_buf(inbuf).await? == 0 {
                    return Ok(None);
                }
            }
        }
    }
}

// ── Asynchronous diagnostics and notifications ─────────────────────────────

fn write_notices(out: &mut BytesMut, notices: Option<&mut mpsc::Receiver<PgError>>) {
    if let Some(rx) = notices {
        while let Ok(notice) = rx.try_recv() {
            backend::notice_response(out, &notice);
        }
    }
}

/// Write the startup `ParameterStatus` burst for one connection.
///
/// The burst is the server's static set, with two things laid over it. The
/// engine's own [`Session::reported_parameters`] win outright, because the
/// engine is what applied the startup packet and what normalised it. For the
/// [`VERBATIM_REPORTED_PARAMS`] an engine that reports nothing still gets the
/// client's own value echoed, which is safe precisely because those two are
/// stored unchanged. A reported parameter the static set does not carry is
/// announced after it, so an engine can grow the set without the wire layer
/// changing.
fn announce_startup_parameters<Sess: Session>(
    out: &mut BytesMut,
    parameters: &mut ParameterReporter,
    config: &SessionConfig,
    startup_params: &[(String, String)],
    session: &Sess,
) {
    let from_engine = session.reported_parameters();
    let lookup = |named: &str, pairs: &[(String, String)]| {
        pairs
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(named))
            .map(|(_, value)| value.clone())
    };
    for (name, value) in &config.server_params {
        let announced = from_engine
            .iter()
            .find(|reported| reported.name.eq_ignore_ascii_case(name))
            .map(|reported| reported.value.clone())
            .or_else(|| {
                VERBATIM_REPORTED_PARAMS
                    .iter()
                    .any(|verbatim| verbatim.eq_ignore_ascii_case(name))
                    .then(|| lookup(name, startup_params))
                    .flatten()
            })
            .unwrap_or_else(|| value.clone());
        parameters.announce(out, name, &announced);
    }
    for reported in &from_engine {
        parameters.announce(out, &reported.name, &reported.value);
    }
}

/// Write the `ReadyForQuery` that closes an exchange, preceded by any pending
/// `NoticeResponse`, `NotificationResponse` and `ParameterStatus` messages.
///
/// Postgres delivers notifications only between transactions. A session that
/// is idle *in* a transaction block accumulates them until the block ends, so
/// this function drains the queue only when the reported status is `Idle`.
///
/// Changed `GUC_REPORT` parameters go out last, immediately ahead of
/// `ReadyForQuery`, which is where a backend puts them:
/// `ReportChangedGUCOptions` is the last thing `PostgresMain` does before it
/// calls `ReadyForQuery`. That placement is what batches a multi-statement
/// `Query` or a pipeline into one report per round instead of one per
/// statement — measured on 18.4, `SELECT 1; SET application_name='a2';
/// SELECT 2` puts its single `ParameterStatus` after the *last* statement's
/// `CommandComplete`.
fn write_ready<Sess: Session>(
    out: &mut BytesMut,
    session: &Sess,
    notices: Option<&mut mpsc::Receiver<PgError>>,
    notifications: Option<&mut mpsc::Receiver<Notification>>,
    parameters: &mut ParameterReporter,
) {
    write_notices(out, notices);
    let status = session.tx_status();
    if status == TxStatus::Idle
        && let Some(rx) = notifications
    {
        while let Ok(notification) = rx.try_recv() {
            backend::notification_response(
                out,
                notification.process_id,
                &notification.channel,
                &notification.payload,
            );
        }
    }
    parameters.flush(out);
    backend::ready_for_query(out, status);
}

/// Wait for more frontend bytes and push notifications that arrive meanwhile.
///
/// Returns `false` when the client went away. This function awaits
/// notifications only between transactions. See [`write_ready`]. Inside a
/// transaction block it is a plain read, and the queue drains at the next
/// `ReadyForQuery`. The caller passes `status` by value rather than read it
/// from the session, so the returned future borrows nothing shared and stays
/// `Send` for a non-`Sync` session.
async fn read_or_notify<S>(
    stream: &mut S,
    inbuf: &mut BytesMut,
    out: &mut BytesMut,
    status: TxStatus,
    notifications: Option<&mut mpsc::Receiver<Notification>>,
) -> std::io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let Some(rx) = notifications.filter(|_| status == TxStatus::Idle) else {
        return Ok(stream.read_buf(inbuf).await? > 0);
    };
    loop {
        // Both branches are cancellation-safe: `read_buf` keeps whatever it
        // already appended to `inbuf`, and `recv` loses no queued notification.
        let notification = tokio::select! {
            read = stream.read_buf(inbuf) => return Ok(read? > 0),
            notification = rx.recv() => notification,
        };
        // The engine dropped the sender (no more notifications will ever
        // arrive): fall back to a plain read instead of spinning on `None`.
        let Some(notification) = notification else {
            return Ok(stream.read_buf(inbuf).await? > 0);
        };
        backend::notification_response(
            out,
            notification.process_id,
            &notification.channel,
            &notification.payload,
        );
        stream.write_all(out).await?;
        out.clear();
        stream.flush().await?;
    }
}

// ── Main session loop ───────────────────────────────────────────────────────

/// Drive a single connection from the point immediately after the decode of
/// the `StartupMessage`.
///
/// `inbuf` is the residual buffer from the pre-startup negotiation phase, which
/// `server::handle_conn` owns. Any bytes the client pipelined immediately after
/// the startup packet are already in `inbuf`. The caller passes `inbuf` here so
/// that the session does not silently drop those bytes.
///
/// # Errors
///
/// Returns an I/O error when a read, a write, an authentication step, or the
/// processing of a protocol message fails.
///
/// # Panics
///
/// Panics if an internal COPY state or cancellation-registry invariant is
/// violated.
#[expect(
    clippy::too_many_lines,
    reason = "session loop mirrors the protocol state machine"
)]
pub async fn run_session<S, E>(
    mut stream: S,
    startup_params: Vec<(String, String)>,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    cancel: SessionCancel,
    mut inbuf: BytesMut,
    activity: SessionActivity,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    E: Engine,
{
    let mut out = BytesMut::with_capacity(1024);

    // The enclosing `gres.session` span is created by `server::serve_conn`,
    // which is where the peer address lives; the startup packet is the first
    // point at which the rest of its attributes are known. A session driven
    // without that wrapper simply records onto a disabled span.
    let session_span = tracing::Span::current();
    telemetry::record_session_startup(&session_span, &startup_params, cancel.pid);

    if !authenticate(&mut stream, &startup_params, &config, &mut out, &mut inbuf).await? {
        return Ok(());
    }
    // One session per connection; it owns the (currently trivial) transaction
    // state and is threaded by `&mut` through the message loop. The pid it is
    // built with is the one announced in `BackendKeyData` below, so a
    // self-notify reaches the client stamped with its own pid.
    //
    // Nothing is announced until the engine has taken the startup packet and
    // finished starting up, because until then there is nothing truthful to
    // announce: the engine applies these parameters as the session's defaults
    // and normalises them on the way in. It is also what a backend does with a
    // startup packet it rejects — a bad GUC there is fatal *before* the
    // `ParameterStatus` burst, so the client gets `ErrorResponse` and nothing
    // else.
    let mut session = engine.connect_with_pid(cancel.pid);
    // `database` is not a GUC, so it does not go through `startup_parameter`;
    // it is the session's identity and the engine is told it outright. Dropping
    // it here is what made every session believe it was in `postgres`.
    if let Some((_, database)) = startup_params
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("database"))
    {
        session.set_database(database);
    }
    for (name, value) in &startup_params {
        if !matches!(name.as_str(), "user" | "database")
            && let Err(error) = session.startup_parameter(name, value).await
        {
            backend::error_response(&mut out, &error);
            stream.write_all(&out).await?;
            session.terminate().await;
            return Ok(());
        }
    }
    if let Err(error) = session.startup().await {
        backend::error_response(&mut out, &error);
        stream.write_all(&out).await?;
        session.terminate().await;
        return Ok(());
    }
    // The loop, not the session, owns the asynchronous streams: taking them
    // once here avoids a second borrow of `session` during protocol handling.
    let mut notifications = session.take_notifications();
    let mut notices = session.take_notices();
    let mut parameters = ParameterReporter {
        changes: session.take_parameter_changes(),
        ..ParameterReporter::default()
    };
    announce_startup_parameters(
        &mut out,
        &mut parameters,
        &config,
        &startup_params,
        &session,
    );
    backend::backend_key_data(&mut out, cancel.pid, cancel.secret);

    write_ready(
        &mut out,
        &session,
        notices.as_mut(),
        notifications.as_mut(),
        &mut parameters,
    );
    stream.write_all(&out).await?;
    out.clear();

    // Always terminate the engine session, regardless of how the protocol loop exits.
    let outcome: std::io::Result<()> = async {
        let mut ext = ExtendedState::default();
        let mut copy_in: Option<CopyInState> = None;

        loop {
            let msg =
                match frontend::decode_message_with_max_len(&mut inbuf, config.max_message_len) {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        if !read_or_notify(
                            &mut stream,
                            &mut inbuf,
                            &mut out,
                            session.tx_status(),
                            notifications.as_mut(),
                        )
                        .await?
                        {
                            return Ok(()); // client went away
                        }
                        continue;
                    }
                    Err(e) => {
                        backend::error_response(&mut out, &e);
                        stream.write_all(&out).await?;
                        return Ok(()); // protocol errors are fatal
                    }
                };

            if let Some(state) = &mut copy_in {
                match msg {
                    FrontendMessage::CopyData(data) => {
                        state.chunks.push(data);
                        continue;
                    }
                    FrontendMessage::CopyDone => {
                        let CopyInState { target, chunks } =
                            copy_in.take().expect("copy state present");
                        let extended = matches!(target, CopyInTarget::Portal { .. });
                        let _statement_activity = activity.begin_statement().await;
                        let token = cancel.begin_query();
                        let outcome = tokio::select! {
                            biased;
                            () = token.cancelled() => None,
                            r = async {
                                match &target {
                                    CopyInTarget::Statement { sql } => {
                                        session.copy_in(sql, 0, chunks).await
                                    }
                                    CopyInTarget::Batched { sql, statement_index } => {
                                        session.copy_in(sql, *statement_index, chunks).await
                                    }
                                    CopyInTarget::Portal { name } => {
                                        session.copy_in_portal(name, chunks).await
                                    }
                                }
                            } => Some(r),
                        };
                        let outcome = if let Some(outcome) = outcome {
                            outcome
                        } else {
                            session.cancel_current_query().await;
                            Err(query_canceled())
                        };
                        write_notices(&mut out, notices.as_mut());
                        let copy_succeeded = matches!(outcome, Ok(QueryResult::Command { .. }));
                        match outcome {
                            Ok(QueryResult::Command { tag }) => {
                                backend::command_complete(&mut out, &tag);
                            }
                            Ok(_) => {
                                let e = PgError::error(
                                    sqlstate::PROTOCOL_VIOLATION,
                                    "COPY returned rows",
                                );
                                if extended {
                                    fail_extended(&mut ext, &mut out, &e);
                                } else {
                                    backend::error_response(&mut out, &e);
                                }
                            }
                            Err(e) => {
                                if extended {
                                    fail_extended(&mut ext, &mut out, &e);
                                } else {
                                    backend::error_response(&mut out, &e);
                                }
                            }
                        }
                        // A copy the engine stopped at part-way through a query
                        // string leaves the rest of that string to run, and the
                        // rest may hold another copy. Resume until the string is
                        // finished or stops again; only then is anything owed.
                        if copy_succeeded
                            && let CopyInTarget::Batched {
                                sql,
                                statement_index,
                            } = &target
                        {
                            let span = telemetry::statement_span(StatementProtocol::Simple);
                            let batch = run_simple_batch(
                                &mut session,
                                sql,
                                statement_index + 1,
                                &cancel,
                                &span,
                                &mut notices,
                                &mut out,
                            )
                            .await;
                            match batch {
                                BatchOutcome::CopyIn {
                                    statement_index: next,
                                } => {
                                    stream.write_all(&out).await?;
                                    out.clear();
                                    copy_in = Some(CopyInState {
                                        target: CopyInTarget::Batched {
                                            sql: sql.clone(),
                                            statement_index: next,
                                        },
                                        chunks: Vec::new(),
                                    });
                                    continue;
                                }
                                BatchOutcome::Fatal(e) => {
                                    telemetry::record_error(&session_span, &e);
                                    stream.write_all(&out).await?;
                                    return Ok(());
                                }
                                BatchOutcome::Done | BatchOutcome::Failed => {}
                            }
                        }
                        // Extended protocol: the client's Sync (sent after
                        // CopyDone) produces ReadyForQuery; simple protocol owes
                        // it now.
                        if !extended {
                            write_ready(
                                &mut out,
                                &session,
                                notices.as_mut(),
                                notifications.as_mut(),
                                &mut parameters,
                            );
                        }
                        stream.write_all(&out).await?;
                        out.clear();
                        continue;
                    }
                    FrontendMessage::CopyFail(message) => {
                        let state = copy_in.take().expect("copy state present");
                        let extended = matches!(state.target, CopyInTarget::Portal { .. });
                        session.mark_statement_failed();
                        let e = PgError::error(
                            sqlstate::QUERY_CANCELED,
                            format!("COPY failed: {message}"),
                        );
                        if extended {
                            fail_extended(&mut ext, &mut out, &e);
                        } else {
                            backend::error_response(&mut out, &e);
                            write_ready(
                                &mut out,
                                &session,
                                notices.as_mut(),
                                notifications.as_mut(),
                                &mut parameters,
                            );
                        }
                        stream.write_all(&out).await?;
                        out.clear();
                        continue;
                    }
                    // PostgreSQL ignores Flush and Sync during copy-in mode
                    // (drivers pipeline Bind/Execute/Sync before streaming data);
                    // the Sync that matters arrives after CopyDone/CopyFail.
                    FrontendMessage::Sync | FrontendMessage::Flush => continue,
                    FrontendMessage::Terminate => return Ok(()),
                    _ => {
                        let e = PgError::protocol("unexpected frontend message during COPY");
                        backend::error_response(&mut out, &e);
                        stream.write_all(&out).await?;
                        return Ok(());
                    }
                }
            }

            match msg {
                FrontendMessage::Terminate => return Ok(()),
                FrontendMessage::Query { sql } => {
                    let _statement_activity = activity.begin_statement().await;
                    let statement_span = telemetry::statement_span(StatementProtocol::Simple);
                    // The simple protocol carries its SQL, so this is where a
                    // sqlcommenter tag is read. The text itself is left alone:
                    // the parser skips comments without emitting a token, and it
                    // keeps the original string so a syntax error's byte offset
                    // still points where the client expects.
                    let _ingress =
                        telemetry::ingress_from_sql(config.ingress_trace, &sql, &statement_span);
                    match begin_simple_copy(&mut session, &sql, &cancel).await {
                        Ok(SimpleCopy::In(response)) => {
                            write_notices(&mut out, notices.as_mut());
                            backend::copy_in_response(
                                &mut out,
                                response.overall_format,
                                &response.column_formats,
                            );
                            stream.write_all(&out).await?;
                            out.clear();
                            copy_in = Some(CopyInState {
                                target: CopyInTarget::Statement { sql },
                                chunks: Vec::new(),
                            });
                            continue;
                        }
                        Ok(SimpleCopy::Out(copy)) => {
                            write_notices(&mut out, notices.as_mut());
                            write_copy_out(&mut out, &copy);
                            write_ready(
                                &mut out,
                                &session,
                                notices.as_mut(),
                                notifications.as_mut(),
                                &mut parameters,
                            );
                            stream.write_all(&out).await?;
                            out.clear();
                            continue;
                        }
                        Ok(SimpleCopy::None) => {}
                        Err(e) => {
                            telemetry::record_statement_error(&statement_span, &e);
                            write_notices(&mut out, notices.as_mut());
                            backend::error_response(&mut out, &e);
                            write_ready(
                                &mut out,
                                &session,
                                notices.as_mut(),
                                notifications.as_mut(),
                                &mut parameters,
                            );
                            stream.write_all(&out).await?;
                            out.clear();
                            continue;
                        }
                    }
                    let batch = run_simple_batch(
                        &mut session,
                        &sql,
                        0,
                        &cancel,
                        &statement_span,
                        &mut notices,
                        &mut out,
                    )
                    .await;
                    match batch {
                        BatchOutcome::CopyIn { statement_index } => {
                            stream.write_all(&out).await?;
                            out.clear();
                            copy_in = Some(CopyInState {
                                target: CopyInTarget::Batched {
                                    sql,
                                    statement_index,
                                },
                                chunks: Vec::new(),
                            });
                            continue;
                        }
                        BatchOutcome::Fatal(e) => {
                            // A fatal diagnostic ends the connection, so it is
                            // the session's outcome too — the only thing that
                            // marks `gres.session` failed.
                            telemetry::record_error(&session_span, &e);
                            stream.write_all(&out).await?;
                            return Ok(());
                        }
                        BatchOutcome::Done | BatchOutcome::Failed => {}
                    }
                    write_ready(
                        &mut out,
                        &session,
                        notices.as_mut(),
                        notifications.as_mut(),
                        &mut parameters,
                    );
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Sync => {
                    let result = session.sync().await;
                    write_notices(&mut out, notices.as_mut());
                    if let Err(e) = result {
                        backend::error_response(&mut out, &e);
                    }
                    ext.failed = false;
                    // A statement that outlives a `Sync` is genuinely being
                    // reused; the trace it was prepared under is stale.
                    ext.trace = TraceCarrier::default();
                    write_ready(
                        &mut out,
                        &session,
                        notices.as_mut(),
                        notifications.as_mut(),
                        &mut parameters,
                    );
                    stream.write_all(&out).await?;
                    out.clear();
                }
                // Every arm write_all()s eagerly, so there is never pending response data; Flush has nothing to drain and TcpStream::flush is a no-op.
                FrontendMessage::Flush => stream.flush().await?,
                FrontendMessage::FunctionCall(call) => {
                    if ext.failed {
                        continue;
                    }
                    let result = if session.tx_status() == TxStatus::Failed {
                        Err(PgError::error(
                            sqlstate::IN_FAILED_SQL_TRANSACTION,
                            "current transaction is aborted, commands ignored until end of \
                             transaction block",
                        ))
                    } else {
                        session.fastpath(call).await
                    };
                    write_notices(&mut out, notices.as_mut());
                    match result {
                        Ok(value) => backend::function_call_response(&mut out, value),
                        Err(error) => {
                            session.mark_statement_failed();
                            backend::error_response(&mut out, &error);
                        }
                    }
                    write_ready(
                        &mut out,
                        &session,
                        notices.as_mut(),
                        notifications.as_mut(),
                        &mut parameters,
                    );
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => {
                    if ext.failed {
                        continue;
                    }
                    let prepared = handle_parse(
                        &mut session,
                        name,
                        sql,
                        param_types,
                        config.ingress_trace,
                        &mut out,
                    )
                    .await;
                    match prepared {
                        Ok(carrier) => ext.trace = carrier,
                        Err(e) => fail_extended(&mut ext, &mut out, &e),
                    }
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    params,
                    result_formats,
                } => {
                    if ext.failed {
                        continue;
                    }
                    if let Err(e) = handle_bind(
                        &mut session,
                        portal,
                        &statement,
                        &param_formats,
                        params,
                        &result_formats,
                        &mut out,
                    )
                    .await
                    {
                        fail_extended(&mut ext, &mut out, &e);
                    }
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Describe { kind, name } => {
                    if ext.failed {
                        continue;
                    }
                    if let Err(e) = handle_describe(&mut session, kind, &name, &mut out).await {
                        fail_extended(&mut ext, &mut out, &e);
                    }
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    if ext.failed {
                        continue;
                    }
                    let _statement_activity = activity.begin_statement().await;
                    let statement_span = telemetry::statement_span(StatementProtocol::Extended);
                    config
                        .ingress_trace
                        .attach(resolve_execute_parent(&ext), &statement_span);
                    // Cancel window: between extended messages no engine future runs; the pending flag in CancelRegistry makes a cancel received there fire on the next engine call.
                    let token = cancel.begin_query();
                    let executed = handle_execute(
                        &mut session,
                        &portal,
                        max_rows,
                        token,
                        notices.as_mut(),
                        &mut out,
                    )
                    .instrument(statement_span.clone())
                    .await;
                    match executed {
                        Ok(Some(copy_start)) => copy_in = Some(copy_start),
                        Ok(None) => {}
                        Err(e) => {
                            telemetry::record_statement_error(&statement_span, &e);
                            write_notices(&mut out, notices.as_mut());
                            fail_extended(&mut ext, &mut out, &e);
                        }
                    }
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Close { kind, name } => {
                    if ext.failed {
                        continue;
                    }
                    let result = match kind {
                        b'S' => session.close(CloseTarget::Statement(&name)).await,
                        b'P' => session.close(CloseTarget::Portal(&name)).await,
                        _ => {
                            let e =
                                PgError::protocol(format!("invalid close kind {:?}", kind as char));
                            fail_extended(&mut ext, &mut out, &e);
                            stream.write_all(&out).await?;
                            out.clear();
                            continue;
                        }
                    };
                    if let Err(e) = result {
                        fail_extended(&mut ext, &mut out, &e);
                        stream.write_all(&out).await?;
                        out.clear();
                        continue;
                    }
                    backend::close_complete(&mut out);
                    stream.write_all(&out).await?;
                    out.clear();
                }
                FrontendMessage::Password(_) => {
                    let e = PgError::protocol("unexpected password message outside authentication");
                    backend::error_response(&mut out, &e);
                    stream.write_all(&out).await?;
                    return Ok(());
                }
                // Accepted and ignored, as Postgres does: a COPY that failed
                // leaves the frontend still streaming frames it had already
                // queued, and killing the connection over them would strand a
                // client that is about to recover on its own.
                FrontendMessage::CopyData(_)
                | FrontendMessage::CopyDone
                | FrontendMessage::CopyFail(_) => {}
            }
        }
    }
    .await;
    session.terminate().await;
    outcome
}

struct WireResultSink<'a> {
    out: &'a mut BytesMut,
    notices: Option<&'a mut mpsc::Receiver<PgError>>,
    /// Rows written to the wire so far. The session folds this onto
    /// `gres.statement` once the statement finishes. The sink itself raises no
    /// spans, because a 100k-row result would emit a hundred page spans that
    /// the exporter discards.
    rows: usize,
    /// Row pages written so far, the companion to [`WireResultSink::rows`].
    pages: usize,
}

#[async_trait::async_trait]
impl ResultSink for WireResultSink<'_> {
    async fn send(&mut self, page: ResultPage) -> Result<(), PgError> {
        write_notices(self.out, self.notices.as_deref_mut());
        match page {
            ResultPage::Rows {
                fields, rows, tag, ..
            } => {
                self.rows += rows.len();
                self.pages += 1;
                if let Some(fields) = fields {
                    backend::row_description(self.out, &fields);
                }
                for row in rows {
                    let values = row
                        .into_iter()
                        .map(|cell| cell.map(|cell| cell.text))
                        .collect::<Vec<_>>();
                    backend::data_row(self.out, &values);
                }
                if let Some(tag) = tag {
                    backend::command_complete(self.out, &tag);
                }
            }
            ResultPage::Command { tag, .. } => backend::command_complete(self.out, &tag),
            ResultPage::Empty { .. } => backend::empty_query_response(self.out),
            ResultPage::CopyOut { stream, .. } => {
                self.rows += stream.rows.len();
                self.pages += 1;
                write_copy_out(self.out, &stream);
            }
        }
        Ok(())
    }

    async fn send_notice(&mut self, notice: PgError) -> Result<(), PgError> {
        backend::notice_response(self.out, &notice);
        Ok(())
    }
}

#[cfg(test)]
mod execute_outcome_tests {
    use assert2::assert;

    use super::*;
    use crate::engine::{CopyOutResponse, Notification};

    #[test]
    fn reserved_execute_outcomes_are_shaped_feature_errors() {
        let outcomes = [
            ExecuteOutcome::CopyIn {
                response: CopyInResponse {
                    overall_format: 0,
                    column_formats: vec![],
                },
            },
            ExecuteOutcome::Notification {
                notification: Notification {
                    process_id: 1,
                    channel: "c".into(),
                    payload: "p".into(),
                },
            },
        ];
        for outcome in outcomes {
            let error = encode_execute_outcome(&mut BytesMut::new(), outcome)
                .expect_err("reserved outcome must fail");
            assert!(error.code == sqlstate::FEATURE_NOT_SUPPORTED);
        }
    }

    /// The bytes a pinned `PostgreSQL` 18.4 backend put on the wire for
    /// `COPY t TO STDOUT` over a two-column table holding `(1, 'one')`,
    /// `(2, NULL)` and `(3, 'th<tab>ree')`.
    fn postgres_copy_out_block() -> &'static [u8] {
        b"H\x00\x00\x00\x0b\x00\x00\x02\x00\x00\x00\x00\
          d\x00\x00\x00\x0a1\tone\n\
          d\x00\x00\x00\x092\t\\N\n\
          d\x00\x00\x00\x0e3\tth\\tree\n\
          c\x00\x00\x00\x04\
          C\x00\x00\x00\x0bCOPY 3\0"
    }

    fn sample_copy_out() -> CopyOutStream {
        CopyOutStream {
            response: CopyOutResponse {
                overall_format: 0,
                column_formats: vec![0, 0],
            },
            rows: vec![
                Bytes::from_static(b"1\tone\n"),
                Bytes::from_static(b"2\t\\N\n"),
                Bytes::from_static(b"3\tth\\tree\n"),
            ],
            tag: "COPY 3".into(),
        }
    }

    #[test]
    fn copy_out_execute_outcome_encodes_the_whole_postgres_block() {
        let mut out = BytesMut::new();
        encode_execute_outcome(
            &mut out,
            ExecuteOutcome::CopyOut {
                stream: sample_copy_out(),
            },
        )
        .expect("copy-out encodes");

        assert!(&out[..] == postgres_copy_out_block());
    }

    #[test]
    fn empty_copy_out_still_frames_a_response_terminator_and_tag() {
        let mut out = BytesMut::new();
        write_copy_out(
            &mut out,
            &CopyOutStream {
                response: CopyOutResponse {
                    overall_format: 0,
                    column_formats: vec![0],
                },
                rows: Vec::new(),
                tag: "COPY 0".into(),
            },
        );

        // Postgres answers `COPY <empty table> TO STDOUT` with exactly these
        // three messages and no CopyData at all.
        assert!(
            &out[..]
                == &b"H\x00\x00\x00\x09\x00\x00\x01\x00\x00c\x00\x00\x00\x04C\x00\x00\x00\x0bCOPY 0\0"[..]
        );
    }

    #[test]
    fn binary_copy_out_marks_every_column_binary() {
        let mut out = BytesMut::new();
        write_copy_out(
            &mut out,
            &CopyOutStream {
                response: CopyOutResponse {
                    overall_format: 1,
                    column_formats: vec![1, 1],
                },
                rows: vec![Bytes::from_static(b"\xff\xff")],
                tag: "COPY 0".into(),
            },
        );

        assert!(
            &out[..]
                == &b"H\x00\x00\x00\x0b\x01\x00\x02\x00\x01\x00\x01d\x00\x00\x00\x06\xff\xffc\x00\x00\x00\x04C\x00\x00\x00\x0bCOPY 0\0"[..]
        );
    }
}
