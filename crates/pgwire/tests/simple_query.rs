use std::sync::Arc;

use crabka_pgwire::{session::SessionConfig, stub::StubEngine};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

#[derive(Clone)]
struct CopyEngine;

struct CopySession {
    tx: CopyTx,
}

#[derive(Clone, Copy)]
enum CopyTx {
    Idle,
    InTransaction,
    Failed,
}

impl crabka_pgwire::engine::Engine for CopyEngine {
    type Session = CopySession;

    fn connect(&self) -> Self::Session {
        CopySession { tx: CopyTx::Idle }
    }
}

impl crabka_pgwire::engine::Session for CopySession {
    async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<crabka_pgwire::engine::QueryResult>, crabka_pgwire::error::PgError> {
        if matches!(self.tx, CopyTx::Failed) && sql != "ROLLBACK" {
            return Err(crabka_pgwire::error::PgError::error(
                crabka_pgwire::error::sqlstate::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        if sql == "BEGIN" {
            self.tx = CopyTx::InTransaction;
            return Ok(vec![crabka_pgwire::engine::QueryResult::Command {
                tag: "BEGIN".into(),
            }]);
        }
        if sql == "ROLLBACK" {
            self.tx = CopyTx::Idle;
            return Ok(vec![crabka_pgwire::engine::QueryResult::Command {
                tag: "ROLLBACK".into(),
            }]);
        }
        if sql == "SELECT 1" {
            return Ok(vec![crabka_pgwire::engine::QueryResult::Rows {
                fields: vec![crabka_pgwire::engine::FieldDescription {
                    name: "?column?".into(),
                    table_oid: 0,
                    column_id: 0,
                    type_oid: crabka_pgwire::engine::oids::INT4,
                    type_size: 4,
                    type_modifier: -1,
                    format: 0,
                }],
                rows: vec![vec![Some(crabka_pgwire::engine::Cell {
                    text: bytes::Bytes::from_static(b"1"),
                    binary: bytes::Bytes::copy_from_slice(&1i32.to_be_bytes()),
                })]],
                tag: "SELECT 1".into(),
            }]);
        }
        Err(crabka_pgwire::error::PgError::error(
            crabka_pgwire::error::sqlstate::FEATURE_NOT_SUPPORTED,
            "unsupported",
        ))
    }

    async fn describe(
        &mut self,
        _sql: &str,
    ) -> Result<Vec<crabka_pgwire::engine::FieldDescription>, crabka_pgwire::error::PgError> {
        Ok(Vec::new())
    }

    async fn begin_copy_in(
        &mut self,
        sql: &str,
    ) -> Result<Option<crabka_pgwire::engine::CopyInResponse>, crabka_pgwire::error::PgError> {
        if sql != "COPY t FROM STDIN" {
            return Ok(None);
        }
        if matches!(self.tx, CopyTx::Failed) {
            return Err(crabka_pgwire::error::PgError::error(
                crabka_pgwire::error::sqlstate::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        Ok(Some(crabka_pgwire::engine::CopyInResponse {
            overall_format: 0,
            column_formats: vec![0],
        }))
    }

    async fn copy_in(
        &mut self,
        _sql: &str,
        data: Vec<bytes::Bytes>,
    ) -> Result<crabka_pgwire::engine::QueryResult, crabka_pgwire::error::PgError> {
        let rows = data
            .iter()
            .flat_map(|chunk| chunk.iter())
            .filter(|byte| **byte == b'\n')
            .count();
        Ok(crabka_pgwire::engine::QueryResult::Command {
            tag: format!("COPY {rows}"),
        })
    }

    fn tx_status(&self) -> crabka_pgwire::engine::TxStatus {
        match self.tx {
            CopyTx::Idle => crabka_pgwire::engine::TxStatus::Idle,
            CopyTx::InTransaction => crabka_pgwire::engine::TxStatus::InTransaction,
            CopyTx::Failed => crabka_pgwire::engine::TxStatus::Failed,
        }
    }

    fn mark_statement_failed(&mut self) {
        if matches!(self.tx, CopyTx::InTransaction) {
            self.tx = CopyTx::Failed;
        }
    }
}

async fn spawn_copy_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(CopyEngine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

fn put_message(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
    out.push(tag);
    let len = i32::try_from(body.len() + 4).expect("message length fits");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
}

async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    stream
        .read_exact(&mut header)
        .await
        .expect("message header");
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    let body_len = usize::try_from(len - 4).expect("positive length");
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body).await.expect("message body");
    (header[0], body)
}

async fn read_tag(stream: &mut TcpStream) -> u8 {
    read_message(stream).await.0
}

async fn read_ready_status(stream: &mut TcpStream) -> u8 {
    let (tag, body) = read_message(stream).await;
    assert_eq!(tag, b'Z');
    assert_eq!(body.len(), 1);
    body[0]
}

async fn raw_connect(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    body.extend_from_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = Vec::new();
    let len = i32::try_from(body.len() + 4).expect("startup length fits");
    startup.extend_from_slice(&len.to_be_bytes());
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("startup");
    while read_tag(&mut stream).await != b'Z' {}
    stream
}

#[tokio::test]
async fn trust_auth_and_select_1() {
    let client = connect(spawn_server().await).await;
    let messages = client.simple_query("SELECT 1").await.expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert_eq!(row.get(0), Some("1"));
}

#[tokio::test]
async fn version_query_works() {
    let client = connect(spawn_server().await).await;
    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert!(row.get(0).expect("value").starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn unsupported_query_returns_0a000_and_session_survives() {
    let client = connect(spawn_server().await).await;
    let err = client
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000");
    // The session must still be usable after an ERROR (not FATAL).
    let messages = client
        .simple_query("SELECT 1")
        .await
        .expect("session survives");
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn empty_query_returns_cleanly() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres surfaces EmptyQueryResponse as zero rows
    let messages = client.simple_query("   ").await.expect("empty ok");
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn three_sequential_queries_on_one_session() {
    let client = connect(spawn_server().await).await;
    for _ in 0..3 {
        let messages = client.simple_query("SELECT 1").await.expect("query");
        let row = messages
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .expect("one row");
        assert_eq!(row.get(0), Some("1"));
    }
}

#[tokio::test]
async fn raw_copy_from_stdin_success_then_query_recovery() {
    let mut stream = raw_connect(spawn_copy_server().await).await;
    let mut out = Vec::new();
    put_message(&mut out, b'Q', b"COPY t FROM STDIN\0");
    stream.write_all(&out).await.expect("query");
    assert_eq!(read_tag(&mut stream).await, b'G');

    let mut copy = Vec::new();
    put_message(&mut copy, b'd', b"1\n2\n");
    put_message(&mut copy, b'c', b"");
    stream.write_all(&copy).await.expect("copy data");
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'I');

    let mut query = Vec::new();
    put_message(&mut query, b'Q', b"SELECT 1\0");
    stream.write_all(&query).await.expect("select");
    assert_eq!(read_tag(&mut stream).await, b'T');
    assert_eq!(read_tag(&mut stream).await, b'D');
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'I');
}

#[tokio::test]
async fn raw_copy_fail_discards_and_recovers() {
    let mut stream = raw_connect(spawn_copy_server().await).await;
    let mut out = Vec::new();
    put_message(&mut out, b'Q', b"COPY t FROM STDIN\0");
    stream.write_all(&out).await.expect("query");
    assert_eq!(read_tag(&mut stream).await, b'G');

    let mut fail = Vec::new();
    put_message(&mut fail, b'f', b"client aborted\0");
    stream.write_all(&fail).await.expect("copy fail");
    assert_eq!(read_tag(&mut stream).await, b'E');
    assert_eq!(read_ready_status(&mut stream).await, b'I');

    let mut query = Vec::new();
    put_message(&mut query, b'Q', b"SELECT 1\0");
    stream.write_all(&query).await.expect("select");
    assert_eq!(read_tag(&mut stream).await, b'T');
    assert_eq!(read_tag(&mut stream).await, b'D');
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'I');
}

#[tokio::test]
async fn raw_copy_fail_in_transaction_aborts_until_rollback() {
    let mut stream = raw_connect(spawn_copy_server().await).await;

    let mut begin = Vec::new();
    put_message(&mut begin, b'Q', b"BEGIN\0");
    stream.write_all(&begin).await.expect("begin");
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'T');

    let mut copy_query = Vec::new();
    put_message(&mut copy_query, b'Q', b"COPY t FROM STDIN\0");
    stream.write_all(&copy_query).await.expect("copy query");
    assert_eq!(read_tag(&mut stream).await, b'G');

    let mut fail = Vec::new();
    put_message(&mut fail, b'f', b"client aborted\0");
    stream.write_all(&fail).await.expect("copy fail");
    assert_eq!(read_tag(&mut stream).await, b'E');
    assert_eq!(read_ready_status(&mut stream).await, b'E');

    let mut blocked_select = Vec::new();
    put_message(&mut blocked_select, b'Q', b"SELECT 1\0");
    stream
        .write_all(&blocked_select)
        .await
        .expect("blocked select");
    assert_eq!(read_tag(&mut stream).await, b'E');
    assert_eq!(read_ready_status(&mut stream).await, b'E');

    let mut rollback = Vec::new();
    put_message(&mut rollback, b'Q', b"ROLLBACK\0");
    stream.write_all(&rollback).await.expect("rollback");
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'I');

    let mut recovered_select = Vec::new();
    put_message(&mut recovered_select, b'Q', b"SELECT 1\0");
    stream
        .write_all(&recovered_select)
        .await
        .expect("recovered select");
    assert_eq!(read_tag(&mut stream).await, b'T');
    assert_eq!(read_tag(&mut stream).await, b'D');
    assert_eq!(read_tag(&mut stream).await, b'C');
    assert_eq!(read_ready_status(&mut stream).await, b'I');
}
