use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, CopyOutResponse, Engine, ExecuteOutcome, FieldDescription,
        PortalDescription, PreparedDescription, QueryResult, Session, TxStatus, oids,
    },
    error::{PgError, sqlstate},
    session::SessionConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{NoTls, types::Type};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Parse(String, String, Vec<u32>),
    Bind(String, String, Vec<BoundParam>, Vec<i16>),
    Execute(String, u32),
    Sync,
}

#[derive(Clone, Default)]
struct RecordingEngine {
    calls: Arc<Mutex<Vec<Call>>>,
}

struct RecordingSession {
    calls: Arc<Mutex<Vec<Call>>>,
    prepared: HashMap<String, PreparedDescription>,
    portals: HashMap<String, usize>,
    slow: HashSet<String>,
    tx: TxStatus,
}

impl Engine for RecordingEngine {
    type Session = RecordingSession;
    fn connect(&self) -> Self::Session {
        RecordingSession {
            calls: Arc::clone(&self.calls),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            slow: HashSet::new(),
            tx: TxStatus::Idle,
        }
    }
}

impl Session for RecordingSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql == "BEGIN" {
            self.tx = TxStatus::InTransaction;
        }
        Ok(vec![QueryResult::Command { tag: sql.into() }])
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        self.calls.lock().expect("calls").push(Call::Parse(
            name.into(),
            sql.into(),
            parameter_types.to_vec(),
        ));
        let description = PreparedDescription {
            parameter_types: parameter_types.to_vec(),
            fields: vec![text_field()],
        };
        self.prepared.insert(name.into(), description.clone());
        Ok(description)
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, PgError> {
        self.calls.lock().expect("calls").push(Call::Bind(
            portal.into(),
            statement.into(),
            params.to_vec(),
            result_formats.to_vec(),
        ));
        if !self.prepared.contains_key(statement) {
            return Err(PgError::error(
                sqlstate::INVALID_SQL_STATEMENT_NAME,
                "missing",
            ));
        }
        self.portals.insert(portal.into(), 0);
        if portal == "slow" {
            self.slow.insert(portal.into());
        }
        Ok(PortalDescription {
            fields: vec![text_field()],
        })
    }

    async fn describe_statement(&mut self, name: &str) -> Result<PreparedDescription, PgError> {
        self.prepared
            .get(name)
            .cloned()
            .ok_or_else(|| PgError::error(sqlstate::INVALID_SQL_STATEMENT_NAME, "missing"))
    }
    async fn describe_portal(&mut self, name: &str) -> Result<PortalDescription, PgError> {
        self.portals
            .get(name)
            .map(|_| PortalDescription {
                fields: vec![text_field()],
            })
            .ok_or_else(|| PgError::error(sqlstate::INVALID_CURSOR_NAME, "missing"))
    }
    async fn execute(&mut self, portal: &str, max_rows: u32) -> Result<ExecuteOutcome, PgError> {
        self.calls
            .lock()
            .expect("calls")
            .push(Call::Execute(portal.into(), max_rows));
        if portal == "reserved" {
            return Ok(ExecuteOutcome::CopyOut {
                response: CopyOutResponse {
                    overall_format: 0,
                    column_formats: vec![0],
                },
            });
        }
        if self.slow.contains(portal) {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        let position = self
            .portals
            .get_mut(portal)
            .ok_or_else(|| PgError::error(sqlstate::INVALID_CURSOR_NAME, "missing"))?;
        let values = ["1", "2", "3", "4"];
        let remaining = values.len() - *position;
        let take = if max_rows == 0 {
            remaining
        } else {
            remaining.min(max_rows as usize)
        };
        let rows = values[*position..*position + take]
            .iter()
            .map(|v| vec![Some(Bytes::copy_from_slice(v.as_bytes()))])
            .collect();
        *position += take;
        Ok(ExecuteOutcome::Rows {
            rows,
            completion: (*position == values.len()).then(|| "SELECT 4".into()),
        })
    }
    async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        match target {
            CloseTarget::Statement(n) => {
                self.prepared.remove(n);
            }
            CloseTarget::Portal(n) => {
                self.portals.remove(n);
            }
        }
        Ok(())
    }
    async fn sync(&mut self) -> Result<(), PgError> {
        self.calls.lock().expect("calls").push(Call::Sync);
        self.portals.clear();
        Ok(())
    }
    fn mark_statement_failed(&mut self) {
        if self.tx == TxStatus::InTransaction {
            self.tx = TxStatus::Failed;
        }
    }
    fn tx_status(&self) -> TxStatus {
        self.tx
    }
}

fn text_field() -> FieldDescription {
    FieldDescription {
        name: "value".into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::TEXT,
        type_size: -1,
        type_modifier: -1,
        format: 0,
    }
}

async fn spawn(engine: RecordingEngine) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(engine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(engine: RecordingEngine) -> tokio_postgres::Client {
    let port = spawn(engine).await;
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

fn tagged(tag: u8, body: &[u8]) -> BytesMut {
    let mut frame = BytesMut::new();
    frame.put_u8(tag);
    frame.put_i32(i32::try_from(body.len()).expect("length") + 4);
    frame.put_slice(body);
    frame
}

async fn read_backend(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.expect("tag");
    let len = stream.read_i32().await.expect("len");
    let mut body = vec![0; usize::try_from(len - 4).expect("body len")];
    stream.read_exact(&mut body).await.expect("body");
    (tag, body)
}

async fn raw_connect(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let mut body = BytesMut::new();
    body.put_i32(0x0003_0000);
    body.put_slice(b"user\0crab\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len()).expect("length") + 4);
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("startup");
    while read_backend(&mut stream).await.0 != b'Z' {}
    stream
}

#[tokio::test]
async fn pgwire_forwards_one_parse_three_binds_and_name_only_executes() {
    let engine = RecordingEngine::default();
    let calls = Arc::clone(&engine.calls);
    let client = connect(engine).await;
    let statement = client
        .prepare_typed("SELECT $1", &[Type::TEXT])
        .await
        .expect("prepare");
    for value in ["a", "b", "c"] {
        client.query(&statement, &[&value]).await.expect("query");
    }
    let calls = calls.lock().expect("calls");
    assert_eq!(
        calls
            .iter()
            .filter(|c| matches!(c, Call::Parse(_, _, _)))
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| matches!(c, Call::Bind(_, _, _, _)))
            .count(),
        3
    );
    let executes = calls
        .iter()
        .filter_map(|c| {
            if let Call::Execute(portal, max) = c {
                Some((portal, max))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(executes.len(), 3);
    assert!(executes.iter().all(|(_, max)| **max == 0));
    let bound_portals = calls
        .iter()
        .filter_map(|c| {
            if let Call::Bind(portal, _, _, _) = c {
                Some(portal)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        executes
            .iter()
            .map(|(portal, _)| *portal)
            .collect::<Vec<_>>(),
        bound_portals
    );
}

#[tokio::test]
async fn engine_owns_slicing_sync_and_close_lifetimes_inside_begin() {
    let engine = RecordingEngine::default();
    let mut session = engine.connect();
    session.simple_query("BEGIN").await.expect("begin");
    session.parse("s", "SELECT 1", &[]).await.expect("parse");
    session.bind("p", "s", &[], &[]).await.expect("bind");
    let mut shapes = Vec::new();
    for max_rows in [2, 1, 0] {
        let ExecuteOutcome::Rows { rows, completion } =
            session.execute("p", max_rows).await.expect("execute")
        else {
            panic!("rows")
        };
        shapes.push((rows.len(), completion));
    }
    assert_eq!(
        shapes,
        vec![(2, None), (1, None), (1, Some("SELECT 4".into()))]
    );
    session
        .close(CloseTarget::Statement("s"))
        .await
        .expect("close statement");
    session
        .execute("p", 0)
        .await
        .expect("bound portal survives statement close");
    session
        .close(CloseTarget::Statement("missing"))
        .await
        .expect("missing close succeeds");
    session
        .parse("survivor", "SELECT 1", &[])
        .await
        .expect("parse survivor");
    session
        .bind("gone", "survivor", &[], &[])
        .await
        .expect("bind gone");
    session.sync().await.expect("sync inside begin");
    assert_eq!(session.tx_status(), TxStatus::InTransaction);
    assert_eq!(
        session
            .execute("gone", 0)
            .await
            .expect_err("portal removed")
            .code,
        sqlstate::INVALID_CURSOR_NAME
    );
    session
        .bind("after", "survivor", &[], &[])
        .await
        .expect("prepared survives");
}

#[tokio::test]
async fn canceled_execute_does_not_advance_cursor_and_fails_explicit_transaction() {
    let engine = RecordingEngine::default();
    let mut session = engine.connect();
    session.simple_query("BEGIN").await.expect("begin");
    session.parse("s", "SELECT 1", &[]).await.expect("parse");
    session.bind("slow", "s", &[], &[]).await.expect("bind");
    let canceled =
        tokio::time::timeout(Duration::from_millis(10), session.execute("slow", 1)).await;
    assert!(canceled.is_err(), "execute future was canceled");
    session.mark_statement_failed();
    assert_eq!(session.tx_status(), TxStatus::Failed);
    assert_eq!(
        session.portals.get("slow"),
        Some(&0),
        "cursor did not advance"
    );
}

#[tokio::test]
async fn reserved_outcome_is_0a000_skips_until_sync_then_recovers() {
    let port = spawn(RecordingEngine::default()).await;
    let mut stream = raw_connect(port).await;
    let mut parse = BytesMut::new();
    parse.put_slice(b"s\0SELECT 1\0");
    parse.put_i16(0);
    let mut bind = BytesMut::new();
    bind.put_slice(b"reserved\0s\0");
    bind.put_i16(0);
    bind.put_i16(0);
    bind.put_i16(0);
    let mut execute = BytesMut::new();
    execute.put_slice(b"reserved\0");
    execute.put_i32(0);
    let mut ignored_parse = BytesMut::new();
    ignored_parse.put_slice(b"ignored\0SELECT 1\0");
    ignored_parse.put_i16(0);
    let mut batch = BytesMut::new();
    batch.extend_from_slice(&tagged(b'P', &parse));
    batch.extend_from_slice(&tagged(b'B', &bind));
    batch.extend_from_slice(&tagged(b'E', &execute));
    batch.extend_from_slice(&tagged(b'P', &ignored_parse));
    batch.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&batch).await.expect("batch");
    assert_eq!(read_backend(&mut stream).await.0, b'1');
    assert_eq!(read_backend(&mut stream).await.0, b'2');
    let (tag, error) = read_backend(&mut stream).await;
    assert_eq!(tag, b'E');
    assert!(error.windows(6).any(|window| window == b"0A000\0"));
    assert_eq!(
        read_backend(&mut stream).await.0,
        b'Z',
        "ignored Parse emitted no response"
    );

    let mut recovery_parse = BytesMut::new();
    recovery_parse.put_slice(b"recovered\0SELECT 1\0");
    recovery_parse.put_i16(0);
    let mut recovery = BytesMut::new();
    recovery.extend_from_slice(&tagged(b'P', &recovery_parse));
    recovery.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&recovery).await.expect("recovery");
    assert_eq!(read_backend(&mut stream).await.0, b'1');
    assert_eq!(read_backend(&mut stream).await.0, b'Z');
}
