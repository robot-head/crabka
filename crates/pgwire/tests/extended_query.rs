use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use crabka_pgwire::{session::SessionConfig, stub::StubEngine};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{NoTls, types::Type};

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

fn frame_len(len: usize) -> i32 {
    i32::try_from(len).expect("test frame length fits in i32") + 4
}

fn tagged(tag: u8, body: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(tag);
    buf.put_i32(frame_len(body.len()));
    buf.put_slice(body);
    buf
}

async fn read_backend(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.expect("backend tag");
    let len = stream.read_i32().await.expect("backend length");
    assert!(len >= 4, "backend length is self-inclusive");
    let body_len = usize::try_from(len - 4).expect("positive body length");
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body).await.expect("backend body");
    (tag, body)
}

fn assert_single_text_data_row(body: &[u8], expected_value: &[u8]) {
    let mut body = body;
    assert_eq!(body.get_i16(), 1, "data row has one column");
    let value_len = usize::try_from(body.get_i32()).expect("text value length is positive");
    assert_eq!(body, expected_value, "data row text value");
    assert_eq!(value_len, expected_value.len(), "data row text length");
}

fn assert_command_complete(body: &[u8], expected_tag: &[u8]) {
    assert_eq!(body, expected_tag);
}

async fn raw_connect(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("raw connect");
    let mut startup_body = BytesMut::new();
    startup_body.put_i32(0x0003_0000);
    startup_body.put_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(frame_len(startup_body.len()));
    startup.put_slice(&startup_body);
    stream.write_all(&startup).await.expect("startup");

    loop {
        let (tag, _) = read_backend(&mut stream).await;
        if tag == b'Z' {
            return stream;
        }
    }
}

#[tokio::test]
async fn prepare_and_query_select_1_binary_format() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres uses Parse/Describe/Bind/Execute and requests BINARY results.
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    let rows = client.query(&stmt, &[]).await.expect("query");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn version_via_extended_protocol() {
    let client = connect(spawn_server().await).await;
    let rows = client.query("SELECT version()", &[]).await.expect("query");
    let v: &str = rows[0].get(0);
    assert!(v.starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn parameterized_extended_query_returns_bound_text_parameter() {
    let client = connect(spawn_server().await).await;
    let stmt = client
        .prepare_typed("SELECT $1", &[Type::TEXT])
        .await
        .expect("prepare");
    let rows = client.query(&stmt, &[&"crab"]).await.expect("query");
    let value: &str = rows[0].get(0);
    assert_eq!(value, "crab");
}

#[tokio::test]
async fn parameterized_extended_query_returns_bound_int_parameter() {
    let client = connect(spawn_server().await).await;
    let stmt = client
        .prepare_typed("SELECT $1", &[Type::INT4])
        .await
        .expect("prepare");
    let rows = client.query(&stmt, &[&42_i32]).await.expect("query");
    let value: i32 = rows[0].get(0);
    assert_eq!(value, 42);
}

#[tokio::test]
async fn parameterized_extended_query_preserves_int_null_parameter() {
    let client = connect(spawn_server().await).await;
    let stmt = client
        .prepare_typed("SELECT $1", &[Type::INT4])
        .await
        .expect("prepare");
    let value: Option<i32> = None;
    let rows = client.query(&stmt, &[&value]).await.expect("query");
    let returned: Option<i32> = rows[0].get(0);
    assert_eq!(returned, None);
}

#[tokio::test]
async fn parameterized_extended_query_preserves_null_parameter() {
    let client = connect(spawn_server().await).await;
    let stmt = client
        .prepare_typed("SELECT $1", &[Type::TEXT])
        .await
        .expect("prepare");
    let value: Option<&str> = None;
    let rows = client.query(&stmt, &[&value]).await.expect("query");
    let returned: Option<&str> = rows[0].get(0);
    assert_eq!(returned, None);
}

#[tokio::test]
async fn error_skips_until_sync_and_session_recovers() {
    let client = connect(spawn_server().await).await;
    let err = client
        .query("SELECT * FROM nope", &[])
        .await
        .expect_err("must fail");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "0A000");
    // tokio-postgres sends Sync after the failed exchange; a healthy
    // implementation recovers and serves the next query.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn reusing_a_prepared_statement_works() {
    let client = connect(spawn_server().await).await;
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    for _ in 0..3 {
        let rows = client.query(&stmt, &[]).await.expect("query");
        let v: i32 = rows[0].get(0);
        assert_eq!(v, 1);
    }
}

#[tokio::test]
async fn execute_returns_affected_count_path() {
    let client = connect(spawn_server().await).await;
    // execute() returns the CommandComplete row count for the Rows path.
    let n = client.execute("SELECT 1", &[]).await.expect("execute");
    assert_eq!(n, 1);
}

#[tokio::test]
async fn empty_query_via_extended_protocol() {
    let client = connect(spawn_server().await).await;
    // Parse("") → describe → NoData; Execute → EmptyQueryResponse.
    // tokio-postgres surfaces EmptyQueryResponse as an Ok result with zero rows.
    let rows = client.query("", &[]).await.expect("empty ok");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn execute_max_rows_suspends_and_continues_portal() {
    let mut stream = raw_connect(spawn_server().await).await;

    let mut parse_body = BytesMut::new();
    parse_body.put_slice(b"stmt\0SELECT generate_series(1, 3)\0");
    parse_body.put_i16(0);

    let mut bind_body = BytesMut::new();
    bind_body.put_slice(b"portal\0stmt\0");
    bind_body.put_i16(0);
    bind_body.put_i16(0);
    bind_body.put_i16(0);

    let mut execute_one_body = BytesMut::new();
    execute_one_body.put_slice(b"portal\0");
    execute_one_body.put_i32(2);

    let mut execute_all_body = BytesMut::new();
    execute_all_body.put_slice(b"portal\0");
    execute_all_body.put_i32(0);

    let mut messages = BytesMut::new();
    messages.extend_from_slice(&tagged(b'P', &parse_body));
    messages.extend_from_slice(&tagged(b'B', &bind_body));
    messages.extend_from_slice(&tagged(b'E', &execute_one_body));
    messages.extend_from_slice(&tagged(b'E', &execute_all_body));
    messages.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&messages).await.expect("extended batch");

    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'1');
    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'2');
    let (tag, body) = read_backend(&mut stream).await;
    assert_eq!(tag, b'D');
    assert_single_text_data_row(&body, b"1");
    let (tag, body) = read_backend(&mut stream).await;
    assert_eq!(tag, b'D');
    assert_single_text_data_row(&body, b"2");
    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b's');
    let (tag, body) = read_backend(&mut stream).await;
    assert_eq!(tag, b'D');
    assert_single_text_data_row(&body, b"3");
    let (tag, body) = read_backend(&mut stream).await;
    assert_eq!(tag, b'C');
    assert_command_complete(&body, b"SELECT 3\0");
    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'Z');
}

#[tokio::test]
async fn describe_statement_reports_inferred_unknown_parameter() {
    let mut stream = raw_connect(spawn_server().await).await;

    let mut parse_body = BytesMut::new();
    parse_body.put_slice(b"stmt\0SELECT $1\0");
    parse_body.put_i16(0);

    let mut describe_body = BytesMut::new();
    describe_body.put_u8(b'S');
    describe_body.put_slice(b"stmt\0");

    let mut messages = BytesMut::new();
    messages.extend_from_slice(&tagged(b'P', &parse_body));
    messages.extend_from_slice(&tagged(b'D', &describe_body));
    messages.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&messages).await.expect("extended batch");

    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'1');
    let (tag, body) = read_backend(&mut stream).await;
    assert_eq!(tag, b't');
    let mut body = body.as_slice();
    assert_eq!(body.get_i16(), 1);
    assert_eq!(body.get_i32(), 0);
    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'T');
    let (tag, _) = read_backend(&mut stream).await;
    assert_eq!(tag, b'Z');
}

#[tokio::test]
async fn bind_parameter_count_error_skips_until_sync_then_recovers() {
    let mut stream = raw_connect(spawn_server().await).await;

    let mut parse_body = BytesMut::new();
    parse_body.put_slice(b"stmt\0SELECT $1\0");
    parse_body.put_i16(0);

    let mut bind_body = BytesMut::new();
    bind_body.put_slice(b"portal\0stmt\0");
    bind_body.put_i16(0);
    bind_body.put_i16(0);
    bind_body.put_i16(0);

    let mut execute_body = BytesMut::new();
    execute_body.put_slice(b"portal\0");
    execute_body.put_i32(0);

    let mut messages = BytesMut::new();
    messages.extend_from_slice(&tagged(b'P', &parse_body));
    messages.extend_from_slice(&tagged(b'B', &bind_body));
    messages.extend_from_slice(&tagged(b'E', &execute_body));
    messages.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&messages).await.expect("failing batch");

    let mut tags = Vec::new();
    loop {
        let (tag, _) = read_backend(&mut stream).await;
        tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(tags, vec![b'1', b'E', b'Z']);

    let mut parse_body = BytesMut::new();
    parse_body.put_slice(b"ok\0SELECT 1\0");
    parse_body.put_i16(0);

    let mut bind_body = BytesMut::new();
    bind_body.put_slice(b"\0ok\0");
    bind_body.put_i16(0);
    bind_body.put_i16(0);
    bind_body.put_i16(0);

    let mut execute_body = BytesMut::new();
    execute_body.put_u8(0);
    execute_body.put_i32(0);

    let mut messages = BytesMut::new();
    messages.extend_from_slice(&tagged(b'P', &parse_body));
    messages.extend_from_slice(&tagged(b'B', &bind_body));
    messages.extend_from_slice(&tagged(b'E', &execute_body));
    messages.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&messages).await.expect("recovery batch");

    let mut tags = Vec::new();
    loop {
        let (tag, _) = read_backend(&mut stream).await;
        tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(tags, vec![b'1', b'2', b'D', b'C', b'Z']);
}

#[tokio::test]
async fn close_suspended_portal_clears_execution_state_after_sync() {
    let mut stream = raw_connect(spawn_server().await).await;

    let mut parse_body = BytesMut::new();
    parse_body.put_slice(b"stmt\0SELECT generate_series(1, 3)\0");
    parse_body.put_i16(0);

    let mut bind_body = BytesMut::new();
    bind_body.put_slice(b"portal\0stmt\0");
    bind_body.put_i16(0);
    bind_body.put_i16(0);
    bind_body.put_i16(0);

    let mut execute_limited_body = BytesMut::new();
    execute_limited_body.put_slice(b"portal\0");
    execute_limited_body.put_i32(2);

    let mut suspend_messages = BytesMut::new();
    suspend_messages.extend_from_slice(&tagged(b'P', &parse_body));
    suspend_messages.extend_from_slice(&tagged(b'B', &bind_body));
    suspend_messages.extend_from_slice(&tagged(b'E', &execute_limited_body));
    suspend_messages.extend_from_slice(&tagged(b'S', b""));
    stream
        .write_all(&suspend_messages)
        .await
        .expect("suspending batch");

    let mut suspended_tags = Vec::new();
    loop {
        let (tag, _) = read_backend(&mut stream).await;
        suspended_tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(suspended_tags, vec![b'1', b'2', b'D', b'D', b's', b'Z']);

    let mut close_body = BytesMut::new();
    close_body.put_u8(b'P');
    close_body.put_slice(b"portal\0");

    let mut execute_body = BytesMut::new();
    execute_body.put_slice(b"portal\0");
    execute_body.put_i32(0);

    let mut messages = BytesMut::new();
    messages.extend_from_slice(&tagged(b'C', &close_body));
    messages.extend_from_slice(&tagged(b'E', &execute_body));
    messages.extend_from_slice(&tagged(b'E', &execute_body));
    messages.extend_from_slice(&tagged(b'S', b""));
    stream.write_all(&messages).await.expect("close batch");

    let mut tags = Vec::new();
    loop {
        let (tag, _) = read_backend(&mut stream).await;
        tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(tags, vec![b'3', b'E', b'Z']);

    let mut recovery_bind_body = BytesMut::new();
    recovery_bind_body.put_slice(b"portal\0stmt\0");
    recovery_bind_body.put_i16(0);
    recovery_bind_body.put_i16(0);
    recovery_bind_body.put_i16(0);

    let mut recovery_execute_body = BytesMut::new();
    recovery_execute_body.put_slice(b"portal\0");
    recovery_execute_body.put_i32(0);

    let mut recovery_messages = BytesMut::new();
    recovery_messages.extend_from_slice(&tagged(b'B', &recovery_bind_body));
    recovery_messages.extend_from_slice(&tagged(b'E', &recovery_execute_body));
    recovery_messages.extend_from_slice(&tagged(b'S', b""));
    stream
        .write_all(&recovery_messages)
        .await
        .expect("recovery batch");

    let mut recovery_tags = Vec::new();
    loop {
        let (tag, _) = read_backend(&mut stream).await;
        recovery_tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(recovery_tags, vec![b'2', b'D', b'D', b'D', b'C', b'Z']);
}
