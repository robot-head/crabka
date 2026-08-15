//! `ParameterStatus` for the `GUC_REPORT` parameters, at startup and after.
//!
//! Every expectation was measured against a live `PostgreSQL` 18.4 driven with a
//! raw v3 client, and the startup set is corroborated by
//! `tests/fixtures/psql-select1.trace`, which is a real psql session's burst.

use std::sync::Arc;

use assert2::assert;
use bytes::{BufMut, BytesMut};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, Engine, ExecuteOutcome, PortalDescription, PreparedDescription,
        QueryResult, ReportedParameter, ResultPage, ResultSink, Session, TxStatus,
    },
    error::PgError,
    session::{SessionConfig, default_server_params},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

// ── A stub engine driven entirely by the SQL text ───────────────────────────

/// What the test engine does, per connection.
#[derive(Clone, Default)]
struct Behaviour {
    /// Returned from `Session::reported_parameters`.
    reported: Vec<ReportedParameter>,
    /// A startup parameter with this name is rejected.
    reject_startup: Option<String>,
}

struct ParamEngine {
    behaviour: Behaviour,
}

struct ParamSession {
    behaviour: Behaviour,
    sender: mpsc::Sender<ReportedParameter>,
    receiver: Option<mpsc::Receiver<ReportedParameter>>,
}

impl Engine for ParamEngine {
    type Session = ParamSession;

    fn connect(&self) -> Self::Session {
        let (sender, receiver) = mpsc::channel(16);
        ParamSession {
            behaviour: self.behaviour.clone(),
            sender,
            receiver: Some(receiver),
        }
    }
}

impl ParamSession {
    /// The test's whole SQL dialect: `PUSH name=value` queues a reported
    /// parameter, several are separated by `;`, and anything else is inert.
    fn run(&self, sql: &str) {
        for statement in sql.split(';') {
            if let Some(assignment) = statement.trim().strip_prefix("PUSH ")
                && let Some((name, value)) = assignment.split_once('=')
            {
                self.sender
                    .try_send(ReportedParameter {
                        name: name.trim().to_owned(),
                        value: value.trim().to_owned(),
                    })
                    .expect("parameter receiver remains connected");
            }
        }
    }
}

impl Session for ParamSession {
    async fn startup_parameter(&mut self, name: &str, _: &str) -> Result<(), PgError> {
        match &self.behaviour.reject_startup {
            Some(rejected) if rejected == name => Err(PgError::error(
                "42704",
                format!("unrecognized configuration parameter \"{name}\""),
            )),
            _ => Ok(()),
        }
    }

    fn reported_parameters(&self) -> Vec<ReportedParameter> {
        self.behaviour.reported.clone()
    }

    fn take_parameter_changes(&mut self) -> Option<mpsc::Receiver<ReportedParameter>> {
        self.receiver.take()
    }

    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        self.run(sql);
        Ok(vec![QueryResult::Command {
            tag: "SET".to_owned(),
        }])
    }

    async fn simple_query_into<S: ResultSink>(
        &mut self,
        sql: &str,
        _: usize,
        sink: &mut S,
    ) -> Result<(), PgError> {
        self.run(sql);
        for (result_index, _) in sql.split(';').enumerate() {
            sink.send(ResultPage::Command {
                result_index,
                tag: "SET".to_owned(),
            })
            .await?;
        }
        Ok(())
    }

    async fn parse(&mut self, _: &str, _: &str, _: &[u32]) -> Result<PreparedDescription, PgError> {
        Ok(PreparedDescription {
            parameter_types: Vec::new(),
            fields: Vec::new(),
        })
    }

    async fn bind(
        &mut self,
        _: &str,
        _: &str,
        _: &[BoundParam],
        _: &[i16],
    ) -> Result<PortalDescription, PgError> {
        Ok(PortalDescription { fields: Vec::new() })
    }

    async fn describe_statement(&mut self, _: &str) -> Result<PreparedDescription, PgError> {
        unreachable!("test does not describe statements")
    }

    async fn describe_portal(&mut self, _: &str) -> Result<PortalDescription, PgError> {
        unreachable!("test does not describe portals")
    }

    async fn execute(&mut self, portal: &str, _: u32) -> Result<ExecuteOutcome, PgError> {
        self.run(portal);
        Ok(ExecuteOutcome::CommandComplete {
            tag: "SET".to_owned(),
        })
    }

    async fn close(&mut self, _: CloseTarget<'_>) -> Result<(), PgError> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        Ok(())
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}

// ── Raw protocol helpers ────────────────────────────────────────────────────

async fn spawn_server(behaviour: Behaviour) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("listener address").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(ParamEngine { behaviour }),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

fn frame(tag: u8, body: &[u8]) -> BytesMut {
    let mut frame = BytesMut::new();
    frame.put_u8(tag);
    frame.put_i32(i32::try_from(body.len() + 4).expect("frame length fits"));
    frame.put_slice(body);
    frame
}

async fn read_backend(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.expect("backend tag");
    let length = stream.read_i32().await.expect("backend length");
    let mut body = vec![0; usize::try_from(length - 4).expect("valid backend length")];
    stream.read_exact(&mut body).await.expect("backend body");
    (tag, body)
}

/// Every backend message up to and including `ReadyForQuery`.
async fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let message = read_backend(stream).await;
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

/// Decode the name and value of every `ParameterStatus` in a burst.
fn parameter_statuses(messages: &[(u8, Vec<u8>)]) -> Vec<(String, String)> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'S')
        .map(|(_, body)| {
            let mut fields = body.split(|byte| *byte == 0);
            let name = fields.next().expect("parameter name");
            let value = fields.next().expect("parameter value");
            (
                String::from_utf8(name.to_vec()).expect("utf8 name"),
                String::from_utf8(value.to_vec()).expect("utf8 value"),
            )
        })
        .collect()
}

/// Open a connection and return it with its whole startup burst.
async fn connect_with(port: u16, params: &[(&str, &str)]) -> (TcpStream, Vec<(u8, Vec<u8>)>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let mut body = BytesMut::new();
    body.put_i32(0x0003_0000);
    for (name, value) in params {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len() + 4).expect("startup length fits"));
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("startup");
    let burst = read_until_ready(&mut stream).await;
    (stream, burst)
}

async fn connect(port: u16) -> (TcpStream, Vec<(u8, Vec<u8>)>) {
    connect_with(port, &[("user", "crab"), ("database", "crab")]).await
}

/// Run one simple query and return the messages it produces.
async fn simple_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    stream
        .write_all(&frame(b'Q', &body))
        .await
        .expect("simple query");
    read_until_ready(stream).await
}

// ── The startup burst ───────────────────────────────────────────────────────

/// The burst is the whole static set, in configured order, and every name in it
/// is one `PostgreSQL` marks `GUC_REPORT`. A live 18.4 backend sends fifteen;
/// the three gres withholds — `is_superuser`, `session_authorization` and
/// `scram_iterations` — are the three it cannot answer without inventing
/// something, and each is absent here on purpose.
#[tokio::test]
async fn startup_announces_the_reported_parameters_it_can_answer() {
    let port = spawn_server(Behaviour::default()).await;
    let (_stream, burst) = connect(port).await;

    assert!(
        parameter_statuses(&burst)
            == vec![
                ("server_version".to_owned(), "18.4".to_owned()),
                ("server_encoding".to_owned(), "UTF8".to_owned()),
                ("client_encoding".to_owned(), "UTF8".to_owned()),
                ("application_name".to_owned(), String::new()),
                ("DateStyle".to_owned(), "ISO, MDY".to_owned()),
                ("IntervalStyle".to_owned(), "postgres".to_owned()),
                ("TimeZone".to_owned(), "UTC".to_owned()),
                ("search_path".to_owned(), "\"$user\", public".to_owned()),
                ("default_transaction_read_only".to_owned(), "off".to_owned()),
                ("in_hot_standby".to_owned(), "off".to_owned()),
                ("integer_datetimes".to_owned(), "on".to_owned()),
                ("standard_conforming_strings".to_owned(), "on".to_owned()),
            ]
    );
}

/// `PQserverVersion` parses `server_version`, so it has to name the release
/// `version()` and `server_version_num` name. Those are 18.4.
#[tokio::test]
async fn the_announced_server_version_is_the_one_the_catalog_reports() {
    let announced = default_server_params()
        .into_iter()
        .find(|(name, _)| name == "server_version")
        .map(|(_, value)| value);

    assert!(announced == Some("18.4".to_owned()));
}

/// The burst keeps the backend's own order: `AuthenticationOk`, every
/// `ParameterStatus`, `BackendKeyData`, `ReadyForQuery`.
#[tokio::test]
async fn the_burst_puts_backend_key_data_after_the_parameters() {
    let port = spawn_server(Behaviour::default()).await;
    let (_stream, burst) = connect(port).await;
    let tags: Vec<u8> = burst.iter().map(|(tag, _)| *tag).collect();

    let mut expected = vec![b'R'];
    expected.extend(std::iter::repeat_n(b'S', default_server_params().len()));
    expected.extend_from_slice(b"KZ");
    assert!(tags == expected);
}

/// `application_name` and `search_path` are stored by the engine exactly as the
/// startup packet spelled them, so the burst answers with the client's own
/// value rather than the server default. This is the value a transaction
/// pooler reads back to decide whether a recycled backend is clean.
#[tokio::test]
async fn a_verbatim_startup_parameter_is_announced_as_the_client_sent_it() {
    let port = spawn_server(Behaviour::default()).await;
    let (_stream, burst) = connect_with(
        port,
        &[
            ("user", "crab"),
            ("database", "crab"),
            ("application_name", "f1-client-one"),
            ("search_path", "tenant, public"),
        ],
    )
    .await;
    let announced = parameter_statuses(&burst);

    assert!(
        announced.contains(&("application_name".to_owned(), "f1-client-one".to_owned())),
        "burst was {announced:?}"
    );
    assert!(
        announced.contains(&("search_path".to_owned(), "tenant, public".to_owned())),
        "burst was {announced:?}"
    );
}

/// A startup parameter that normalises is *not* echoed: `PostgreSQL` answers a
/// startup `DateStyle=Postgres` with `Postgres, MDY`, and only the engine knows
/// that. Without an engine that reports one, the static value stands rather
/// than a raw echo that would contradict `SHOW DateStyle`.
#[tokio::test]
async fn a_normalising_startup_parameter_is_not_echoed_raw() {
    let port = spawn_server(Behaviour::default()).await;
    let (_stream, burst) = connect_with(
        port,
        &[
            ("user", "crab"),
            ("database", "crab"),
            ("DateStyle", "Postgres"),
        ],
    )
    .await;

    assert!(parameter_statuses(&burst).contains(&("DateStyle".to_owned(), "ISO, MDY".to_owned())));
}

/// What the engine reports wins over the static set, and a name the static set
/// does not carry is announced too — which is how `session_authorization` and
/// `is_superuser` arrive once the engine can answer them.
#[tokio::test]
async fn engine_reported_parameters_override_and_extend_the_static_set() {
    let port = spawn_server(Behaviour {
        reported: vec![
            ReportedParameter {
                name: "DateStyle".to_owned(),
                value: "Postgres, MDY".to_owned(),
            },
            ReportedParameter {
                name: "session_authorization".to_owned(),
                value: "crab".to_owned(),
            },
        ],
        reject_startup: None,
    })
    .await;
    let (_stream, burst) = connect(port).await;
    let announced = parameter_statuses(&burst);

    assert!(announced.contains(&("DateStyle".to_owned(), "Postgres, MDY".to_owned())));
    assert!(!announced.contains(&("DateStyle".to_owned(), "ISO, MDY".to_owned())));
    assert!(announced.contains(&("session_authorization".to_owned(), "crab".to_owned())));
}

/// A startup packet the engine rejects gets `ErrorResponse` and nothing else.
/// A backend applies the packet's GUCs during its own startup, so a bad one is
/// fatal *before* the burst — the client never sees a `ParameterStatus` or a
/// `BackendKeyData` for a connection that was never established.
#[tokio::test]
async fn a_rejected_startup_parameter_produces_no_burst() {
    let port = spawn_server(Behaviour {
        reported: Vec::new(),
        reject_startup: Some("nonsense".to_owned()),
    })
    .await;
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let mut body = BytesMut::new();
    body.put_i32(0x0003_0000);
    for field in ["user", "crab", "database", "crab", "nonsense", "1"] {
        body.put_slice(field.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len() + 4).expect("startup length fits"));
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("startup");

    let authentication = read_backend(&mut stream).await;
    let error = read_backend(&mut stream).await;
    assert!(authentication.0 == b'R');
    assert!(error.0 == b'E');
}

// ── Mid-session change notification ─────────────────────────────────────────

/// A changed parameter is reported after the statement that changed it and
/// immediately before `ReadyForQuery`, which is where `PostgreSQL` puts it:
/// `ReportChangedGUCOptions` runs last in the round.
#[tokio::test]
async fn a_changed_parameter_is_reported_just_before_ready_for_query() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let messages = simple_query(&mut stream, "PUSH application_name=probe").await;
    let tags: Vec<u8> = messages.iter().map(|(tag, _)| *tag).collect();

    assert!(tags == vec![b'C', b'S', b'Z']);
    assert!(
        parameter_statuses(&messages) == vec![("application_name".to_owned(), "probe".to_owned())]
    );
}

/// A multi-statement `Query` reports once, after the last statement, not once
/// per statement. Measured on 18.4: `SELECT 1; SET application_name='a2';
/// SELECT 2` puts its one `ParameterStatus` after the second `SELECT`.
#[tokio::test]
async fn a_multi_statement_query_reports_once_after_the_last_statement() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let messages = simple_query(&mut stream, "SELECT 1; PUSH application_name=a2; SELECT 2").await;
    let tags: Vec<u8> = messages.iter().map(|(tag, _)| *tag).collect();

    assert!(tags == vec![b'C', b'C', b'C', b'S', b'Z']);
}

/// Two moves of one parameter in a round collapse to the value it ended on.
#[tokio::test]
async fn repeated_moves_in_one_round_collapse_to_the_final_value() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let messages = simple_query(
        &mut stream,
        "PUSH application_name=first; PUSH application_name=second",
    )
    .await;

    assert!(
        parameter_statuses(&messages) == vec![("application_name".to_owned(), "second".to_owned())]
    );
}

/// A value the client has already been told stays silent. `PostgreSQL` keeps a
/// `reported_value` per GUC and compares against it, so rolling a `SET LOCAL`
/// back to the value in force before the transaction sends nothing at all —
/// measured on 18.4, where a `ROLLBACK` that restores the last reported value
/// produces no `ParameterStatus`.
#[tokio::test]
async fn a_move_back_to_the_announced_value_is_silent() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let changed = simple_query(&mut stream, "PUSH application_name=local").await;
    let rolled_back = simple_query(&mut stream, "PUSH application_name=").await;
    let unchanged = simple_query(&mut stream, "PUSH application_name=").await;

    assert!(
        parameter_statuses(&changed) == vec![("application_name".to_owned(), "local".to_owned())]
    );
    // Back to the empty value the startup burst announced.
    assert!(
        parameter_statuses(&rolled_back) == vec![("application_name".to_owned(), String::new())]
    );
    assert!(parameter_statuses(&unchanged) == Vec::new());
}

/// The name keeps the spelling the connection has already used. GUC names are
/// case-insensitive, so an engine may push `datestyle` where the burst said
/// `DateStyle`; a client keying its own map on the name must not end up with
/// two entries for one parameter.
#[tokio::test]
async fn a_reported_name_keeps_the_spelling_the_burst_used() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let messages = simple_query(&mut stream, "PUSH datestyle=Postgres, MDY").await;

    assert!(
        parameter_statuses(&messages) == vec![("DateStyle".to_owned(), "Postgres, MDY".to_owned())]
    );
}

/// The extended protocol reports at `Sync`, and a pipeline of several Executes
/// reports once for the whole pipeline rather than once per Execute.
#[tokio::test]
async fn a_pipeline_reports_once_at_sync() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let mut batch = BytesMut::new();
    for (portal, sql) in [
        ("PUSH application_name=p1", "one"),
        ("PUSH search_path=pg_catalog", "two"),
    ] {
        let mut parse = BytesMut::new();
        parse.put_slice(sql.as_bytes());
        parse.put_u8(0);
        parse.put_slice(sql.as_bytes());
        parse.put_u8(0);
        parse.put_i16(0);
        let mut bind = BytesMut::new();
        bind.put_slice(portal.as_bytes());
        bind.put_u8(0);
        bind.put_slice(sql.as_bytes());
        bind.put_u8(0);
        bind.put_i16(0);
        bind.put_i16(0);
        bind.put_i16(0);
        let mut execute = BytesMut::new();
        execute.put_slice(portal.as_bytes());
        execute.put_u8(0);
        execute.put_i32(0);
        batch.extend_from_slice(&frame(b'P', &parse));
        batch.extend_from_slice(&frame(b'B', &bind));
        batch.extend_from_slice(&frame(b'E', &execute));
    }
    batch.extend_from_slice(&frame(b'S', b""));
    stream.write_all(&batch).await.expect("pipeline");

    let messages = read_until_ready(&mut stream).await;
    let tags: Vec<u8> = messages.iter().map(|(tag, _)| *tag).collect();

    // Parse, Bind, CommandComplete twice over, then one report, then Ready.
    assert!(tags == vec![b'1', b'2', b'C', b'1', b'2', b'C', b'S', b'S', b'Z']);
    assert!(
        parameter_statuses(&messages)
            == vec![
                ("application_name".to_owned(), "p1".to_owned()),
                ("search_path".to_owned(), "pg_catalog".to_owned()),
            ]
    );
}

/// An engine that reports nothing puts nothing extra on the wire, which is what
/// keeps every existing engine's traffic byte-identical.
#[tokio::test]
async fn a_silent_engine_adds_no_messages() {
    let port = spawn_server(Behaviour::default()).await;
    let (mut stream, _) = connect(port).await;

    let messages = simple_query(&mut stream, "SELECT 1").await;
    let tags: Vec<u8> = messages.iter().map(|(tag, _)| *tag).collect();

    assert!(tags == vec![b'C', b'Z']);
}
