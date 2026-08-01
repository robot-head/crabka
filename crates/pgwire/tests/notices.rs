use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, Engine, ExecuteOutcome, PortalDescription, PreparedDescription,
        QueryResult, ResultPage, ResultSink, Session, TxStatus,
    },
    error::PgError,
    session::SessionConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

struct NoticeEngine;

struct NoticeSession {
    sender: mpsc::Sender<PgError>,
    receiver: Option<mpsc::Receiver<PgError>>,
}

impl Engine for NoticeEngine {
    type Session = NoticeSession;

    fn connect(&self) -> Self::Session {
        let (sender, receiver) = mpsc::channel(4);
        NoticeSession {
            sender,
            receiver: Some(receiver),
        }
    }
}

impl Session for NoticeSession {
    async fn simple_query(&mut self, _: &str) -> Result<Vec<QueryResult>, PgError> {
        self.sender
            .try_send(PgError::notice("simple notice"))
            .expect("notice receiver remains connected");
        Ok(vec![QueryResult::Command {
            tag: "DO".to_string(),
        }])
    }

    async fn simple_query_into<S: ResultSink>(
        &mut self,
        sql: &str,
        _: usize,
        sink: &mut S,
    ) -> Result<(), PgError> {
        if sql != "FIRST; NOTICE; SECOND" {
            self.sender
                .try_send(PgError::notice("simple notice"))
                .expect("notice receiver remains connected");
            return sink
                .send(ResultPage::Command {
                    result_index: 0,
                    tag: "DO".to_string(),
                })
                .await;
        }
        sink.send(ResultPage::Command {
            result_index: 0,
            tag: "FIRST".to_string(),
        })
        .await?;
        self.sender
            .try_send(PgError::notice("second statement notice"))
            .expect("notice receiver remains connected");
        sink.send(ResultPage::Command {
            result_index: 1,
            tag: "SECOND".to_string(),
        })
        .await
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

    async fn execute(&mut self, _: &str, _: u32) -> Result<ExecuteOutcome, PgError> {
        self.sender
            .try_send(PgError::warning("extended warning"))
            .expect("notice receiver remains connected");
        Ok(ExecuteOutcome::CommandComplete {
            tag: "DO".to_string(),
        })
    }

    async fn close(&mut self, _: CloseTarget<'_>) -> Result<(), PgError> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        Ok(())
    }

    fn take_notices(&mut self) -> Option<mpsc::Receiver<PgError>> {
        self.receiver.take()
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}

async fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("listener address").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(NoticeEngine),
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

async fn raw_connect(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let mut body = BytesMut::new();
    body.put_i32(0x0003_0000);
    body.put_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(i32::try_from(body.len() + 4).expect("startup length fits"));
    startup.extend_from_slice(&body);
    stream.write_all(&startup).await.expect("startup");
    while read_backend(&mut stream).await.0 != b'Z' {}
    stream
}

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

#[tokio::test]
async fn simple_notice_precedes_command_complete_and_ready_without_error() {
    let mut stream = raw_connect(spawn_server().await).await;
    stream
        .write_all(&frame(b'Q', b"DO NOTICE\0"))
        .await
        .expect("simple query");

    let messages = read_until_ready(&mut stream).await;
    let tags = messages.iter().map(|message| message.0).collect::<Vec<_>>();
    assert2::assert!(tags == vec![b'N', b'C', b'Z']);
    assert2::assert!(!tags.contains(&b'E'));
    assert2::assert!(
        messages[0]
            .1
            .windows(15)
            .any(|field| field == b"Msimple notice\0")
    );
}

#[tokio::test]
async fn later_statement_notice_does_not_precede_earlier_command_complete() {
    let mut stream = raw_connect(spawn_server().await).await;
    stream
        .write_all(&frame(b'Q', b"FIRST; NOTICE; SECOND\0"))
        .await
        .expect("simple query batch");

    let messages = read_until_ready(&mut stream).await;
    let tags = messages.iter().map(|message| message.0).collect::<Vec<_>>();
    assert2::assert!(tags == vec![b'C', b'N', b'C', b'Z']);
    assert2::assert!(
        messages[1]
            .1
            .windows(25)
            .any(|field| field == b"Msecond statement notice\0")
    );
}

#[tokio::test]
async fn extended_notice_precedes_command_complete_and_ready_without_error() {
    let mut stream = raw_connect(spawn_server().await).await;

    let mut parse = BytesMut::new();
    parse.put_slice(b"statement\0DO NOTICE\0");
    parse.put_i16(0);
    let mut bind = BytesMut::new();
    bind.put_slice(b"portal\0statement\0");
    bind.put_i16(0);
    bind.put_i16(0);
    bind.put_i16(0);
    let mut execute = BytesMut::new();
    execute.put_slice(b"portal\0");
    execute.put_i32(0);

    let mut batch = BytesMut::new();
    batch.extend_from_slice(&frame(b'P', &parse));
    batch.extend_from_slice(&frame(b'B', &bind));
    batch.extend_from_slice(&frame(b'E', &execute));
    batch.extend_from_slice(&frame(b'S', b""));
    stream.write_all(&batch).await.expect("extended query");

    let messages = read_until_ready(&mut stream).await;
    let tags = messages.iter().map(|message| message.0).collect::<Vec<_>>();
    assert2::assert!(tags == vec![b'1', b'2', b'N', b'C', b'Z']);
    assert2::assert!(!tags.contains(&b'E'));
    assert2::assert!(
        messages[2]
            .1
            .windows(18)
            .any(|field| field == b"Mextended warning\0")
    );
}
