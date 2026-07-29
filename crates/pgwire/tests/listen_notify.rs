//! Asynchronous `NotificationResponse` delivery through the session loop.
//!
//! A fake engine hands the wire layer a notification stream; the tests drive
//! `run_session` over an in-memory duplex stream and assert on the bytes the
//! client sees.

use std::sync::{Arc, Mutex};

use assert2::assert;
use bytes::BytesMut;
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, Engine, ExecuteOutcome, Notification, PortalDescription,
        PreparedDescription, QueryResult, Session, TxStatus,
    },
    error::{PgError, sqlstate},
    server::{ActivityTracker, CancelRegistry},
    session::{SessionConfig, run_session},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::mpsc,
    time::{Duration, timeout},
};

// ── Fake engine ─────────────────────────────────────────────────────────────

struct NotifyEngine {
    /// Handed to the session so a `NOTIFY` statement can publish while the
    /// wire loop is busy executing it — the only way a notification becomes
    /// pending outside the idle wait.
    publisher: mpsc::Sender<Notification>,
    notifications: Mutex<Option<mpsc::Receiver<Notification>>>,
    connected_pid: Mutex<Option<i32>>,
}

impl NotifyEngine {
    fn new(
        publisher: mpsc::Sender<Notification>,
        notifications: mpsc::Receiver<Notification>,
    ) -> Self {
        Self {
            publisher,
            notifications: Mutex::new(Some(notifications)),
            connected_pid: Mutex::new(None),
        }
    }

    fn connected_pid(&self) -> Option<i32> {
        *self.connected_pid.lock().expect("pid lock")
    }
}

impl Engine for NotifyEngine {
    type Session = NotifySession;

    fn connect(&self) -> Self::Session {
        NotifySession {
            publisher: self.publisher.clone(),
            notifications: None,
            pid: 0,
            tx: TxStatus::Idle,
        }
    }

    fn connect_with_pid(&self, pid: i32) -> Self::Session {
        *self.connected_pid.lock().expect("pid lock") = Some(pid);
        NotifySession {
            publisher: self.publisher.clone(),
            notifications: self.notifications.lock().expect("stream lock").take(),
            pid,
            tx: TxStatus::Idle,
        }
    }
}

struct NotifySession {
    publisher: mpsc::Sender<Notification>,
    notifications: Option<mpsc::Receiver<Notification>>,
    pid: i32,
    tx: TxStatus,
}

fn unsupported() -> PgError {
    PgError::error(sqlstate::FEATURE_NOT_SUPPORTED, "unsupported")
}

impl Session for NotifySession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        let tag = match sql {
            "BEGIN" => {
                self.tx = TxStatus::InTransaction;
                "BEGIN"
            }
            "COMMIT" => {
                self.tx = TxStatus::Idle;
                "COMMIT"
            }
            // `NOTIFY <channel> <payload>`: publish a self-notification while
            // the statement is executing, as a committing engine would.
            _ if sql.starts_with("NOTIFY ") => {
                let mut parts = sql["NOTIFY ".len()..].splitn(2, ' ');
                let channel = parts.next().unwrap_or_default();
                let payload = parts.next().unwrap_or_default();
                self.publisher
                    .send(Notification {
                        process_id: self.pid,
                        channel: channel.into(),
                        payload: payload.into(),
                    })
                    .await
                    .expect("notification queue is open");
                "NOTIFY"
            }
            _ => "SELECT 0",
        };
        Ok(vec![QueryResult::Command { tag: tag.into() }])
    }

    async fn parse(&mut self, _: &str, _: &str, _: &[u32]) -> Result<PreparedDescription, PgError> {
        Err(unsupported())
    }
    async fn bind(
        &mut self,
        _: &str,
        _: &str,
        _: &[BoundParam],
        _: &[i16],
    ) -> Result<PortalDescription, PgError> {
        Err(unsupported())
    }
    async fn describe_statement(&mut self, _: &str) -> Result<PreparedDescription, PgError> {
        Err(unsupported())
    }
    async fn describe_portal(&mut self, _: &str) -> Result<PortalDescription, PgError> {
        Err(unsupported())
    }
    async fn execute(&mut self, _: &str, _: u32) -> Result<ExecuteOutcome, PgError> {
        Err(unsupported())
    }
    async fn close(&mut self, _: CloseTarget<'_>) -> Result<(), PgError> {
        Ok(())
    }
    async fn sync(&mut self) -> Result<(), PgError> {
        Ok(())
    }

    fn take_notifications(&mut self) -> Option<mpsc::Receiver<Notification>> {
        self.notifications.take()
    }

    fn tx_status(&self) -> TxStatus {
        self.tx
    }
}

// ── Wire harness ────────────────────────────────────────────────────────────

struct Harness {
    client: DuplexStream,
    engine: Arc<NotifyEngine>,
    notify: mpsc::Sender<Notification>,
    /// The pid the server announced in `BackendKeyData`.
    key_data_pid: i32,
}

async fn start() -> Harness {
    let (client, server) = tokio::io::duplex(8192);
    let (notify, receiver) = mpsc::channel(16);
    let engine = Arc::new(NotifyEngine::new(notify.clone(), receiver));
    let registry = Arc::new(CancelRegistry::default());
    let activity = Arc::new(ActivityTracker::new())
        .try_open_session()
        .expect("session admission is open");
    tokio::spawn(run_session(
        server,
        Vec::new(),
        Arc::clone(&engine),
        Arc::new(SessionConfig::trust()),
        registry.register(),
        BytesMut::new(),
        activity,
    ));

    let mut harness = Harness {
        client,
        engine,
        notify,
        key_data_pid: 0,
    };
    // Startup burst: AuthenticationOk, ParameterStatus…, BackendKeyData, RFQ.
    loop {
        let (tag, body) = harness.read_message().await;
        match tag {
            b'K' => {
                harness.key_data_pid =
                    i32::from_be_bytes(body[..4].try_into().expect("pid is four bytes"));
            }
            b'Z' => break,
            _ => {}
        }
    }
    harness
}

impl Harness {
    async fn read_message(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0; 5];
        self.client
            .read_exact(&mut header)
            .await
            .expect("message header");
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let body_len = usize::try_from(len - 4).expect("body length is non-negative");
        let mut body = vec![0; body_len];
        self.client
            .read_exact(&mut body)
            .await
            .expect("message body");
        (header[0], body)
    }

    async fn query(&mut self, sql: &str) {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut message = vec![b'Q'];
        let len = i32::try_from(body.len() + 4).expect("message length fits");
        message.extend_from_slice(&len.to_be_bytes());
        message.extend_from_slice(&body);
        self.client.write_all(&message).await.expect("query");
    }

    async fn send_notification(&self, process_id: i32, channel: &str, payload: &str) {
        self.notify
            .send(Notification {
                process_id,
                channel: channel.into(),
                payload: payload.into(),
            })
            .await
            .expect("notification queued");
    }

    /// Read one message, asserting it is a `NotificationResponse`, and return
    /// its decoded (pid, channel, payload).
    async fn read_notification(&mut self) -> (i32, String, String) {
        let (tag, body) = self.read_message().await;
        assert!(tag == b'A');
        let process_id = i32::from_be_bytes(body[..4].try_into().expect("pid is four bytes"));
        let mut parts = body[4..].split(|byte| *byte == 0);
        let channel = String::from_utf8(parts.next().expect("channel").to_vec()).expect("utf8");
        let payload = String::from_utf8(parts.next().expect("payload").to_vec()).expect("utf8");
        // Both strings are NUL-terminated, so the split leaves one empty tail.
        assert!(parts.next() == Some(&[][..]));
        assert!(parts.next() == None);
        (process_id, channel, payload)
    }

    async fn read_ready_status(&mut self) -> u8 {
        let (tag, body) = self.read_message().await;
        assert!(tag == b'Z');
        assert!(body.len() == 1);
        body[0]
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn session_is_connected_with_the_announced_backend_pid() {
    let harness = start().await;

    assert!(harness.engine.connected_pid() == Some(harness.key_data_pid));
}

#[tokio::test]
async fn notification_raised_by_a_statement_precedes_its_ready_for_query() {
    let mut harness = start().await;
    let pid = harness.key_data_pid;

    harness.query("NOTIFY chan queued").await;

    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_notification().await == (pid, "chan".into(), "queued".into()));
    assert!(harness.read_ready_status().await == b'I');
}

#[tokio::test]
async fn notification_reaches_a_connection_parked_on_the_idle_read() {
    let mut harness = start().await;
    let pid = harness.key_data_pid;

    harness.send_notification(pid, "idle", "now").await;

    let delivered = timeout(Duration::from_secs(5), harness.read_notification())
        .await
        .expect("idle connections receive notifications without a client message");
    assert!(delivered == (pid, "idle".into(), "now".into()));

    // The connection is still usable afterwards.
    harness.query("SELECT 0").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_ready_status().await == b'I');
}

#[tokio::test]
async fn notification_is_withheld_inside_a_transaction_block() {
    let mut harness = start().await;
    let pid = harness.key_data_pid;

    harness.query("BEGIN").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_ready_status().await == b'T');

    // Raised inside the block, and again from another backend while the
    // connection sits idle-in-transaction: neither may reach the client yet.
    harness.query("NOTIFY chan held").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_ready_status().await == b'T');
    let idle_in_transaction = timeout(Duration::from_millis(250), harness.read_message()).await;
    assert!(idle_in_transaction.is_err());

    harness.query("COMMIT").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_notification().await == (pid, "chan".into(), "held".into()));
    assert!(harness.read_ready_status().await == b'I');
}

#[tokio::test]
async fn notifications_are_delivered_in_order_and_drained_together() {
    let mut harness = start().await;
    let pid = harness.key_data_pid;

    harness.query("BEGIN").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_ready_status().await == b'T');

    harness.send_notification(pid, "a", "1").await;
    harness.send_notification(pid, "b", "2").await;

    harness.query("COMMIT").await;
    let (tag, _) = harness.read_message().await;
    assert!(tag == b'C');
    assert!(harness.read_notification().await == (pid, "a".into(), "1".into()));
    assert!(harness.read_notification().await == (pid, "b".into(), "2".into()));
    assert!(harness.read_ready_status().await == b'I');
}
