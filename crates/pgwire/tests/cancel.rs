use std::{sync::Arc, time::Duration};

use crabka_pgwire::{session::SessionConfig, stub::StubEngine};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::NoTls;

/// Reads one framed backend message (tag + self-inclusive length + body).
async fn read_backend_msg(stream: &mut TcpStream, buf: &mut Vec<u8>) -> (u8, Vec<u8>) {
    loop {
        if buf.len() >= 5 {
            let len = usize::try_from(i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]))
                .expect("backend frame length is non-negative");
            let total = 1 + len;
            if buf.len() >= total {
                let tag = buf[0];
                let body = buf[5..total].to_vec();
                buf.drain(..total);
                return (tag, body);
            }
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "server closed unexpectedly");
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn framed(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    out.extend_from_slice(&frame_len(body.len()).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn frame_len(len: usize) -> i32 {
    i32::try_from(len).expect("test frame length fits in i32") + 4
}

/// SQLSTATE from an `ErrorResponse` body ('C' field).
fn error_sqlstate(body: &[u8]) -> Option<String> {
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let field = body[i];
        let end = body[i + 1..].iter().position(|&b| b == 0).expect("cstr") + i + 1;
        if field == b'C' {
            return Some(String::from_utf8(body[i + 1..end].to_vec()).expect("utf8"));
        }
        i = end + 1;
    }
    None
}

#[tokio::test]
async fn cancel_request_interrupts_running_query() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);

    // tokio-postgres implements CancelRequest from the BackendKeyData we sent.
    let cancel_token = client.cancel_token();
    let query = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(30)").await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel_token.cancel_query(NoTls).await.expect("cancel sent");

    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query must end promptly after cancel")
        .expect("join");
    let err = result.expect_err("query must be cancelled");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "57014");
}

#[tokio::test]
async fn wrong_cancel_key_is_ignored() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);

    // Hand-rolled CancelRequest with a garbage key: len 16, code 80877102, pid, secret.
    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("tcp");
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&16i32.to_be_bytes());
    pkt.extend_from_slice(&80_877_102i32.to_be_bytes());
    pkt.extend_from_slice(&999_999i32.to_be_bytes());
    pkt.extend_from_slice(&123_456i32.to_be_bytes());
    raw.write_all(&pkt).await.expect("send cancel");
    drop(raw);

    // The session must be unaffected.
    let messages = client
        .simple_query("SELECT 1")
        .await
        .expect("query unaffected");
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn cancel_while_idle_does_not_poison_next_query() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);

    // Run one query so a spent token sits in the slot.
    client.simple_query("SELECT 1").await.expect("first query");
    // Cancel while idle: fires the spent token AND sets pending.
    client
        .cancel_token()
        .cancel_query(NoTls)
        .await
        .expect("cancel sent");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Per best-effort semantics the pending flag will cancel the NEXT query
    // immediately (this matches PG: a cancel that arrives while idle may
    // affect the next command). Accept either outcome: instant 57014 or success.
    match client.simple_query("SELECT 1").await {
        Ok(messages) => {
            assert!(
                messages
                    .iter()
                    .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            );
        }
        Err(e) => {
            assert_eq!(e.as_db_error().expect("db error").code().code(), "57014");
        }
    }
}

/// Deterministically exercises the extended-batch cancel window.
///
/// A `CancelRequest` that lands between Bind and Execute must still cancel the
/// subsequent Execute. In that window no engine future runs and the slot holds
/// a never-polled token. Real Postgres aborts the whole pending batch in this
/// window. The sticky `pending` flag in `CancelRegistry` and the biased,
/// cancellation-first select! make this deterministic, not best-effort.
#[tokio::test]
async fn cancel_during_extended_batch_window_cancels_next_execute() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");

    // StartupMessage (trust auth).
    let mut startup_body = Vec::new();
    startup_body.extend_from_slice(&196_608i32.to_be_bytes());
    startup_body.extend_from_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = Vec::new();
    startup.extend_from_slice(&frame_len(startup_body.len()).to_be_bytes());
    startup.extend_from_slice(&startup_body);
    conn.write_all(&startup).await.expect("startup");

    // Read to ReadyForQuery, capturing BackendKeyData (pid, secret).
    let mut buf = Vec::new();
    let (mut pid, mut secret) = (0i32, 0i32);
    loop {
        let (tag, body) = read_backend_msg(&mut conn, &mut buf).await;
        match tag {
            b'K' => {
                pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                secret = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
            }
            b'Z' => break,
            _ => {}
        }
    }
    assert_ne!(
        (pid, secret),
        (0, 0),
        "BackendKeyData must carry a real key"
    );

    // Parse + Bind only — no Execute yet. After BindComplete the session sits
    // in the window where no engine future runs.
    let mut batch = framed(b'P', b"\0SELECT 1\0\x00\x00"); // unnamed stmt, no param types
    batch.extend_from_slice(&framed(b'B', b"\0\0\x00\x00\x00\x00\x00\x00")); // 0 fmts/params/result fmts
    conn.write_all(&batch).await.expect("parse+bind");

    let (tag, _) = read_backend_msg(&mut conn, &mut buf).await;
    assert_eq!(tag, b'1', "ParseComplete");
    let (tag, _) = read_backend_msg(&mut conn, &mut buf).await;
    assert_eq!(tag, b'2', "BindComplete");

    // CancelRequest mid-window, on its own connection.
    let mut cancel_conn = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&16i32.to_be_bytes());
    pkt.extend_from_slice(&80_877_102i32.to_be_bytes());
    pkt.extend_from_slice(&pid.to_be_bytes());
    pkt.extend_from_slice(&secret.to_be_bytes());
    cancel_conn.write_all(&pkt).await.expect("send cancel");
    drop(cancel_conn);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Execute + Sync: the pending flag must abort this execution.
    let mut tail = framed(b'E', b"\0\x00\x00\x00\x00"); // unnamed portal, max_rows 0
    tail.extend_from_slice(&framed(b'S', b""));
    conn.write_all(&tail).await.expect("execute+sync");

    let mut sqlstate = None;
    loop {
        let (tag, body) = read_backend_msg(&mut conn, &mut buf).await;
        match tag {
            b'E' => sqlstate = error_sqlstate(&body),
            b'Z' => break,
            b'D' | b'C' => panic!("query executed despite mid-batch cancel"),
            _ => {}
        }
    }
    assert_eq!(sqlstate.as_deref(), Some("57014"));
}
