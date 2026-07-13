//! Post-startup connection state machine, generic over the byte stream so the
//! same code runs plaintext and TLS sessions.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::{
    engine::{
        BoundParam, CloseTarget, CopyInResponse, Engine, ExecuteOutcome, QueryResult, Session,
    },
    error::{PgError, Severity, sqlstate},
    messages::{
        backend,
        frontend::{self, FrontendMessage},
    },
    server::{SessionActivity, SessionCancel},
};

#[derive(Debug, Clone)]
pub enum AuthMode {
    Trust,
    /// SCRAM-SHA-256 against stored verifiers (no plaintext at rest). A server
    /// mock secret derives a deterministic fake verifier for unknown users so
    /// the message sequence and timing match a real user — closing the
    /// username-enumeration oracle (RFC 5802 mock authentication).
    ScramSha256 {
        verifiers: std::collections::HashMap<String, crate::scram::ScramVerifier>,
        mock_secret: [u8; 32],
    },
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub auth: AuthMode,
    /// `ParameterStatus` values announced at session start. Clients parse
    /// `server_version` and rely on `client_encoding=UTF8`.
    pub server_params: Vec<(String, String)>,
}

impl SessionConfig {
    #[must_use]
    pub fn trust() -> Self {
        Self {
            auth: AuthMode::Trust,
            server_params: default_server_params(),
        }
    }
}

#[must_use]
pub fn default_server_params() -> Vec<(String, String)> {
    [
        ("server_version", "18.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

// ── Extended-query state ────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct ExtendedState {
    /// True after an error in the extended phase: skip messages until Sync.
    failed: bool,
}

#[derive(Debug)]
struct CopyInState {
    sql: String,
    chunks: Vec<Bytes>,
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

async fn handle_parse<Sess: Session>(
    session: &mut Sess,
    name: String,
    sql: String,
    param_types: Vec<u32>,
    out: &mut BytesMut,
) -> Result<(), PgError> {
    count_positional_parameters(&sql)?;
    session.parse(&name, &sql, &param_types).await?;
    backend::parse_complete(out);
    Ok(())
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
    backend::bind_complete(out);
    Ok(())
}

async fn handle_describe<Sess: Session>(
    session: &mut Sess,
    kind: u8,
    name: &str,
    out: &mut BytesMut,
) -> Result<(), PgError> {
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

async fn handle_execute<Sess: Session>(
    session: &mut Sess,
    portal_name: &str,
    max_rows: i32,
    token: CancellationToken,
    out: &mut BytesMut,
) -> Result<(), PgError> {
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
        () = token.cancelled() => return Err(PgError::error(
            sqlstate::QUERY_CANCELED,
            "canceling statement due to user request",
        )),
        r = session.execute(portal_name, max_rows.cast_unsigned()) => r?,
    };
    encode_execute_outcome(out, outcome)
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
        ExecuteOutcome::CopyIn { .. }
        | ExecuteOutcome::CopyOut { .. }
        | ExecuteOutcome::Notification { .. } => {
            return Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                "execute outcome is reserved for a future protocol extension",
            ));
        }
    }
    Ok(())
}

fn write_copy_in_response(out: &mut BytesMut, response: &CopyInResponse) {
    backend::copy_in_response(out, response.overall_format, &response.column_formats);
}

// ── Authentication helpers ──────────────────────────────────────────────────

/// Runs the authentication exchange. Returns Ok(false) if the client failed
/// authentication (error already written to the stream).
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
        } => {
            let user = startup_params
                .iter()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            let verifier = match verifiers.get(user) {
                Some(v) => v.clone(),
                None => crate::scram::ScramVerifier::mock(mock_secret, user),
            };

            backend::authentication_sasl(out, &["SCRAM-SHA-256"]);
            stream.write_all(out).await?;
            out.clear();

            let Some(mut body) = read_password(stream, inbuf).await? else {
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

            let Some(client_final) = read_password(stream, inbuf).await? else {
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

/// Reads the next frontend message, expecting Password ('p'); returns its body.
async fn read_password<S>(stream: &mut S, inbuf: &mut BytesMut) -> std::io::Result<Option<Bytes>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        match frontend::decode_message(inbuf) {
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

// ── Main session loop ───────────────────────────────────────────────────────

/// Drive a single connection from the point immediately after the `StartupMessage`
/// has been decoded.
///
/// `inbuf` is the residual buffer from the pre-startup negotiation phase (owned
/// by `server::handle_conn`). Any bytes the client pipelined immediately after
/// the startup packet are already in `inbuf`; passing it here avoids silently
/// dropping those bytes.
///
/// # Errors
///
/// Returns an I/O error when reading, writing, authenticating, or processing a
/// protocol message fails.
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

    if !authenticate(&mut stream, &startup_params, &config, &mut out, &mut inbuf).await? {
        return Ok(());
    }
    for (name, value) in &config.server_params {
        backend::parameter_status(&mut out, name, value);
    }
    backend::backend_key_data(&mut out, cancel.pid, cancel.secret);

    // One session per connection; it owns the (currently trivial) transaction
    // state and is threaded by `&mut` through the message loop.
    let mut session = engine.connect();

    backend::ready_for_query(&mut out, session.tx_status());
    stream.write_all(&out).await?;
    out.clear();

    let mut ext = ExtendedState::default();
    let mut copy_in: Option<CopyInState> = None;

    loop {
        let msg = match frontend::decode_message(&mut inbuf) {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                if stream.read_buf(&mut inbuf).await? == 0 {
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
                    let state = copy_in.take().expect("copy state present");
                    activity.touch();
                    let token = cancel.begin_query();
                    let outcome = tokio::select! {
                        biased;
                        () = token.cancelled() => Err(PgError::error(
                            sqlstate::QUERY_CANCELED,
                            "canceling statement due to user request",
                        )),
                        r = session.copy_in(&state.sql, state.chunks) => r,
                    };
                    match outcome {
                        Ok(QueryResult::Command { tag }) => {
                            backend::command_complete(&mut out, &tag);
                        }
                        Ok(_) => backend::error_response(
                            &mut out,
                            &PgError::error(sqlstate::PROTOCOL_VIOLATION, "COPY returned rows"),
                        ),
                        Err(e) => backend::error_response(&mut out, &e),
                    }
                    backend::ready_for_query(&mut out, session.tx_status());
                    stream.write_all(&out).await?;
                    out.clear();
                    continue;
                }
                FrontendMessage::CopyFail(message) => {
                    copy_in = None;
                    session.mark_statement_failed();
                    let e =
                        PgError::error(sqlstate::QUERY_CANCELED, format!("COPY failed: {message}"));
                    backend::error_response(&mut out, &e);
                    backend::ready_for_query(&mut out, session.tx_status());
                    stream.write_all(&out).await?;
                    out.clear();
                    continue;
                }
                FrontendMessage::Sync => {
                    copy_in = None;
                    backend::ready_for_query(&mut out, session.tx_status());
                    stream.write_all(&out).await?;
                    out.clear();
                    continue;
                }
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
                activity.touch();
                let token = cancel.begin_query();
                let copy_start = tokio::select! {
                    biased;
                    () = token.cancelled() => Err(PgError::error(
                        sqlstate::QUERY_CANCELED,
                        "canceling statement due to user request",
                    )),
                    r = session.begin_copy_in(&sql) => r,
                };
                match copy_start {
                    Ok(Some(response)) => {
                        write_copy_in_response(&mut out, &response);
                        stream.write_all(&out).await?;
                        out.clear();
                        copy_in = Some(CopyInState {
                            sql,
                            chunks: Vec::new(),
                        });
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        backend::error_response(&mut out, &e);
                        backend::ready_for_query(&mut out, session.tx_status());
                        stream.write_all(&out).await?;
                        out.clear();
                        continue;
                    }
                }
                let token = cancel.begin_query();
                let outcome = tokio::select! {
                    // biased + cancellation-first; see handle_execute.
                    biased;
                    () = token.cancelled() => Err(PgError::error(
                        sqlstate::QUERY_CANCELED,
                        "canceling statement due to user request",
                    )),
                    r = session.simple_query(&sql) => r,
                };
                match outcome {
                    Ok(results) => write_results(&mut out, &results),
                    Err(e) => {
                        backend::error_response(&mut out, &e);
                        if e.severity == Severity::Fatal {
                            stream.write_all(&out).await?;
                            return Ok(());
                        }
                    }
                }
                backend::ready_for_query(&mut out, session.tx_status());
                stream.write_all(&out).await?;
                out.clear();
            }
            FrontendMessage::Sync => {
                if let Err(e) = session.sync().await {
                    backend::error_response(&mut out, &e);
                }
                ext.failed = false;
                backend::ready_for_query(&mut out, session.tx_status());
                stream.write_all(&out).await?;
                out.clear();
            }
            // Every arm write_all()s eagerly, so there is never pending response data; Flush has nothing to drain and TcpStream::flush is a no-op.
            FrontendMessage::Flush => stream.flush().await?,
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            } => {
                if ext.failed {
                    continue;
                }
                if let Err(e) = handle_parse(&mut session, name, sql, param_types, &mut out).await {
                    fail_extended(&mut ext, &mut out, &e);
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
                activity.touch();
                // Cancel window: between extended messages no engine future runs; the pending flag in CancelRegistry makes a cancel received there fire on the next engine call.
                let token = cancel.begin_query();
                if let Err(e) =
                    handle_execute(&mut session, &portal, max_rows, token, &mut out).await
                {
                    if e.code == sqlstate::QUERY_CANCELED {
                        session.mark_statement_failed();
                    }
                    fail_extended(&mut ext, &mut out, &e);
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
                        let e = PgError::protocol(format!("invalid close kind {:?}", kind as char));
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
            FrontendMessage::CopyData(_)
            | FrontendMessage::CopyDone
            | FrontendMessage::CopyFail(_) => {
                let e = PgError::protocol("COPY message received outside COPY mode");
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(());
            }
        }
    }
}

/// Simple protocol always sends text format.
fn write_results(out: &mut BytesMut, results: &[QueryResult]) {
    for result in results {
        match result {
            QueryResult::Rows { fields, rows, tag } => {
                backend::row_description(out, fields);
                for row in rows {
                    let values: Vec<Option<Bytes>> = row
                        .iter()
                        .map(|c| c.as_ref().map(|c| c.text.clone()))
                        .collect();
                    backend::data_row(out, &values);
                }
                backend::command_complete(out, tag);
            }
            QueryResult::Command { tag } => backend::command_complete(out, tag),
            QueryResult::Empty => backend::empty_query_response(out),
        }
    }
}

#[cfg(test)]
mod execute_outcome_tests {
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
            ExecuteOutcome::CopyOut {
                response: CopyOutResponse {
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
            assert_eq!(error.code, sqlstate::FEATURE_NOT_SUPPORTED);
        }
    }
}
