//! `LISTEN` / `NOTIFY` / `UNLISTEN` and `pg_notify()` across several sessions of
//! one engine, driven through the public `Engine`/`Session` API.
//!
//! The unit tests inside `session.rs` pin the same semantics against the
//! internal wiring, through `SqlSession::register_notify`. This file goes
//! through `Engine::connect_with_pid`, which is what the wire loop calls. It
//! covers the multi-session shapes: who receives what, whose pid is stamped,
//! and what a closed connection does to the bus.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, Notification, QueryResult, Session},
    error::PgError,
};
use tokio::sync::mpsc::{Receiver, error::TryRecvError};

/// A connection registered on the engine's bus under `pid`, plus the receiving
/// end of its notification queue, which is the wire loop's
/// `take_notifications`.
fn connect(engine: &SqlEngine, pid: i32) -> (SqlSession, Receiver<Notification>) {
    let mut session = engine.connect_with_pid(pid);
    let rx = session
        .take_notifications()
        .expect("a notification receiver");
    (session, rx)
}

/// Run one statement and return its command tag.
async fn tag(session: &mut SqlSession, sql: &str) -> String {
    let results = session.simple_query(sql).await.expect(sql);
    let [QueryResult::Command { tag }] = results.as_slice() else {
        panic!("expected exactly one command tag from {sql}");
    };
    tag.clone()
}

/// Run one statement that is expected to fail, and return the error.
async fn error(session: &mut SqlSession, sql: &str) -> PgError {
    session
        .simple_query(sql)
        .await
        .expect_err("the statement should have failed")
}

/// The single text cell of a one-row, one-column result.
fn only_cell(results: &[QueryResult]) -> Option<String> {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected one Rows result, got {results:?}");
    };
    let [row] = rows.as_slice() else {
        panic!("expected exactly one row, got {rows:?}");
    };
    row[0]
        .as_ref()
        .map(|cell: &Cell| String::from_utf8(cell.text.to_vec()).expect("utf8 cell"))
}

fn notification(process_id: i32, channel: &str, payload: &str) -> Notification {
    Notification {
        process_id,
        channel: channel.to_string(),
        payload: payload.to_string(),
    }
}

/// Drain a receiver without blocking.
fn drain(rx: &mut Receiver<Notification>) -> Vec<Notification> {
    let mut got = Vec::new();
    while let Ok(notification) = rx.try_recv() {
        got.push(notification);
    }
    got
}

#[tokio::test]
async fn a_committed_notify_reaches_another_session_and_nothing_arrives_before_the_commit() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    assert!(tag(&mut listener, "LISTEN news").await == "LISTEN");

    tag(&mut notifier, "BEGIN").await;
    assert!(tag(&mut notifier, "NOTIFY news, 'first'").await == "NOTIFY");
    tag(&mut notifier, "NOTIFY news, 'second'").await;
    // Mid-transaction: PostgreSQL queues, it does not deliver.
    assert!(rx.try_recv() == Err(TryRecvError::Empty));

    tag(&mut notifier, "COMMIT").await;
    assert!(
        drain(&mut rx)
            == vec![
                notification(22, "news", "first"),
                notification(22, "news", "second"),
            ]
    );
}

#[tokio::test]
async fn every_listener_of_a_channel_receives_the_same_notification() {
    let engine = SqlEngine::new();
    let (mut first, mut first_rx) = connect(&engine, 11);
    let (mut second, mut second_rx) = connect(&engine, 12);
    let (mut bystander, mut bystander_rx) = connect(&engine, 13);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut first, "LISTEN news").await;
    tag(&mut second, "LISTEN news").await;
    tag(&mut bystander, "LISTEN other").await;

    tag(&mut notifier, "NOTIFY news, 'fan-out'").await;

    assert!(first_rx.try_recv() == Ok(notification(22, "news", "fan-out")));
    assert!(second_rx.try_recv() == Ok(notification(22, "news", "fan-out")));
    assert!(bystander_rx.try_recv() == Err(TryRecvError::Empty));
}

#[tokio::test]
async fn a_rollback_drops_the_queued_notification_and_the_queued_listen() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);

    // A rolled-back NOTIFY never reaches an established listener.
    tag(&mut listener, "LISTEN news").await;
    tag(&mut notifier, "BEGIN").await;
    tag(&mut notifier, "NOTIFY news, 'discarded'").await;
    tag(&mut notifier, "ROLLBACK").await;
    assert!(rx.try_recv() == Err(TryRecvError::Empty));

    // A rolled-back LISTEN leaves no subscription behind, even for the channel
    // the same block also notified.
    let (mut latecomer, mut latecomer_rx) = connect(&engine, 33);
    tag(&mut latecomer, "BEGIN").await;
    tag(&mut latecomer, "LISTEN news").await;
    tag(&mut latecomer, "NOTIFY news, 'self'").await;
    tag(&mut latecomer, "ROLLBACK").await;
    assert!(latecomer_rx.try_recv() == Err(TryRecvError::Empty));
    tag(&mut notifier, "NOTIFY news, 'after'").await;
    assert!(latecomer_rx.try_recv() == Err(TryRecvError::Empty));
    // ... while the session that listened outside a transaction still gets it.
    assert!(rx.try_recv() == Ok(notification(22, "news", "after")));
}

#[tokio::test]
async fn a_rolled_back_unlisten_leaves_the_subscription_intact() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    tag(&mut listener, "BEGIN").await;
    assert!(tag(&mut listener, "UNLISTEN news").await == "UNLISTEN");
    tag(&mut listener, "ROLLBACK").await;

    tag(&mut notifier, "NOTIFY news, 'still here'").await;
    assert!(rx.try_recv() == Ok(notification(22, "news", "still here")));
}

#[tokio::test]
async fn a_failed_statement_aborts_the_block_and_discards_its_notifications() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    tag(&mut notifier, "BEGIN").await;
    tag(&mut notifier, "NOTIFY news, 'doomed'").await;
    assert!(notifier.simple_query("SELECT 1 / 0").await.is_err());
    assert!(tag(&mut notifier, "COMMIT").await == "ROLLBACK");
    assert!(rx.try_recv() == Err(TryRecvError::Empty));

    // The connection recovers: the next transaction delivers normally.
    tag(&mut notifier, "NOTIFY news, 'recovered'").await;
    assert!(rx.try_recv() == Ok(notification(22, "news", "recovered")));
}

#[tokio::test]
async fn duplicates_collapse_within_one_transaction_but_never_across_two() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    tag(&mut notifier, "BEGIN").await;
    for sql in [
        "NOTIFY news, 'a'",
        "NOTIFY news, 'b'",
        "NOTIFY news, 'a'",
        "NOTIFY news",
        "NOTIFY news, 'b'",
        "NOTIFY news",
    ] {
        tag(&mut notifier, sql).await;
    }
    tag(&mut notifier, "COMMIT").await;
    // First occurrence wins the ordering; the empty payload is its own entry.
    assert!(
        drain(&mut rx)
            == vec![
                notification(22, "news", "a"),
                notification(22, "news", "b"),
                notification(22, "news", ""),
            ]
    );

    // A second transaction repeats a pair the first one already sent.
    tag(&mut notifier, "BEGIN").await;
    tag(&mut notifier, "NOTIFY news, 'a'").await;
    tag(&mut notifier, "COMMIT").await;
    tag(&mut notifier, "NOTIFY news, 'a'").await;
    assert!(drain(&mut rx) == vec![notification(22, "news", "a"), notification(22, "news", "a")]);
}

#[tokio::test]
async fn a_session_notifying_a_channel_it_listens_on_receives_its_own_pid() {
    let engine = SqlEngine::new();
    let (mut session, mut rx) = connect(&engine, 77);
    let (mut other, mut other_rx) = connect(&engine, 88);
    tag(&mut session, "LISTEN news").await;
    tag(&mut other, "LISTEN news").await;

    tag(&mut session, "NOTIFY news, 'mine'").await;

    assert!(rx.try_recv() == Ok(notification(77, "news", "mine")));
    // The pid identifies the *notifier*, not the recipient.
    assert!(other_rx.try_recv() == Ok(notification(77, "news", "mine")));
}

#[tokio::test]
async fn listening_and_notifying_in_one_transaction_delivers_to_itself_at_commit() {
    let engine = SqlEngine::new();
    let (mut session, mut rx) = connect(&engine, 55);

    tag(&mut session, "BEGIN").await;
    tag(&mut session, "LISTEN news").await;
    tag(&mut session, "NOTIFY news, 'x'").await;
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
    tag(&mut session, "COMMIT").await;

    assert!(rx.try_recv() == Ok(notification(55, "news", "x")));
}

#[tokio::test]
async fn listening_twice_is_a_no_op_and_delivers_one_copy() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;
    tag(&mut listener, "LISTEN news").await;

    tag(&mut notifier, "NOTIFY news, 'once'").await;

    assert!(drain(&mut rx) == vec![notification(22, "news", "once")]);
}

#[tokio::test]
async fn unlisten_drops_one_channel_and_unlisten_all_drops_every_channel() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN a").await;
    tag(&mut listener, "LISTEN b").await;

    assert!(tag(&mut listener, "UNLISTEN a").await == "UNLISTEN");
    tag(&mut notifier, "NOTIFY a, '1'").await;
    tag(&mut notifier, "NOTIFY b, '2'").await;
    assert!(drain(&mut rx) == vec![notification(22, "b", "2")]);

    assert!(tag(&mut listener, "UNLISTEN *").await == "UNLISTEN");
    tag(&mut notifier, "NOTIFY a, '3'").await;
    tag(&mut notifier, "NOTIFY b, '4'").await;
    assert!(rx.try_recv() == Err(TryRecvError::Empty));

    // UNLISTEN of a channel this session never listened on is a no-op.
    assert!(tag(&mut listener, "UNLISTEN never").await == "UNLISTEN");
}

#[tokio::test]
async fn a_notify_with_no_listeners_succeeds_silently() {
    let engine = SqlEngine::new();
    let (mut notifier, mut rx) = connect(&engine, 22);

    assert!(tag(&mut notifier, "NOTIFY nobody, 'into the void'").await == "NOTIFY");
    // Not even the notifier hears it: it is not listening.
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
}

#[tokio::test]
async fn queueing_a_bad_channel_or_payload_fails_the_notifying_statement() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    // PostgreSQL's limit is `NOTIFY_PAYLOAD_MAX_LENGTH - 1` = 7999 bytes, so
    // 8000 is already over it.
    let oversized_payload = "x".repeat(8000);
    let oversized_channel = "c".repeat(64);
    for sql in [
        format!("NOTIFY news, '{oversized_payload}'"),
        format!(r#"NOTIFY "{oversized_channel}", 'x'"#),
        r#"NOTIFY "", 'x'"#.to_string(),
        format!("SELECT pg_notify('news', '{oversized_payload}')"),
        format!("SELECT pg_notify('{oversized_channel}', 'x')"),
        "SELECT pg_notify('', 'x')".to_string(),
        // pg_notify is not strict: a NULL channel becomes '', which is rejected.
        "SELECT pg_notify(NULL, 'x')".to_string(),
    ] {
        assert!(error(&mut notifier, &sql).await.code == "22023", "{sql}");
    }

    // The rejected statements queued nothing, and the connection still works.
    let just_under = "y".repeat(7999);
    tag(&mut notifier, &format!("NOTIFY news, '{just_under}'")).await;
    assert!(drain(&mut rx) == vec![notification(22, "news", &just_under)]);
}

#[tokio::test]
async fn a_bad_notify_inside_a_transaction_aborts_the_block_without_delivering() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    tag(&mut notifier, "BEGIN").await;
    tag(&mut notifier, "NOTIFY news, 'good'").await;
    let oversized = "x".repeat(8000);
    assert!(
        error(&mut notifier, &format!("NOTIFY news, '{oversized}'"))
            .await
            .code
            == "22023"
    );
    assert!(tag(&mut notifier, "COMMIT").await == "ROLLBACK");
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
}

#[tokio::test]
async fn pg_notify_delivers_in_autocommit_and_at_commit_inside_a_block() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    let results = notifier
        .simple_query("SELECT pg_notify('news', 'function')")
        .await
        .expect("pg_notify");
    // Documented divergence: pg_notify returns text (''), not void.
    assert!(only_cell(&results) == Some(String::new()));
    assert!(rx.try_recv() == Ok(notification(22, "news", "function")));

    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query("SELECT pg_notify('news', 'deferred')")
        .await
        .expect("pg_notify");
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
    tag(&mut notifier, "COMMIT").await;
    assert!(rx.try_recv() == Ok(notification(22, "news", "deferred")));

    // pg_notify and NOTIFY share one dedup set inside a transaction.
    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query("SELECT pg_notify('news', 'shared')")
        .await
        .expect("pg_notify");
    tag(&mut notifier, "NOTIFY news, 'shared'").await;
    tag(&mut notifier, "COMMIT").await;
    assert!(drain(&mut rx) == vec![notification(22, "news", "shared")]);
}

#[tokio::test]
async fn a_rolled_back_pg_notify_delivers_nothing() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;

    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query("SELECT pg_notify('news', 'dropped')")
        .await
        .expect("pg_notify");
    tag(&mut notifier, "ROLLBACK").await;

    assert!(rx.try_recv() == Err(TryRecvError::Empty));
}

#[tokio::test]
async fn closing_a_listening_connection_removes_it_from_the_bus() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;
    assert!(engine.notify_bus().listener_count("news") == 1);

    drop(listener);

    assert!(engine.notify_bus().listener_count("news") == 0);
    // The sending end went with the session, so the orphaned receiver is closed
    // rather than merely empty, and notifying still succeeds.
    tag(&mut notifier, "NOTIFY news, 'nobody home'").await;
    assert!(rx.try_recv() == Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn the_plain_connect_path_is_registered_on_the_bus_and_yields_one_receiver() {
    // `SqlEngine::connect()` draws its own backend pid and registers on the bus
    // with it, so every session of this engine can listen and notify — unlike
    // the gateway's `connect()`, which deliberately refuses. The receiver is
    // handed out exactly once.
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    assert!(session.take_notifications().is_some());
    assert!(session.take_notifications().is_none());
    assert!(tag(&mut session, "LISTEN news").await == "LISTEN");
    assert!(tag(&mut session, "NOTIFY news, 'x'").await == "NOTIFY");
}

#[tokio::test]
async fn notifications_survive_alongside_ordinary_table_work_in_the_same_transaction() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;
    tag(&mut notifier, "CREATE TABLE t (v text)").await;

    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query("INSERT INTO t VALUES ('committed')")
        .await
        .expect("insert");
    tag(&mut notifier, "NOTIFY news, 'with the row'").await;
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
    tag(&mut notifier, "COMMIT").await;
    assert!(rx.try_recv() == Ok(notification(22, "news", "with the row")));

    // A rolled-back write drops its notification too.
    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query("INSERT INTO t VALUES ('rolled back')")
        .await
        .expect("insert");
    tag(&mut notifier, "NOTIFY news, 'with the lost row'").await;
    tag(&mut notifier, "ROLLBACK").await;
    assert!(rx.try_recv() == Err(TryRecvError::Empty));
    let rows = notifier
        .simple_query("SELECT count(*) FROM t")
        .await
        .expect("count");
    assert!(only_cell(&rows) == Some("1".to_string()));
}

/// A batch of `count` distinct notifications queued in one transaction.
fn notify_batch(count: u32) -> String {
    use std::fmt::Write as _;

    (0..count).fold(String::new(), |mut batch, i| {
        write!(batch, "NOTIFY news, 'p{i}';").expect("writing to a String cannot fail");
        batch
    })
}

#[tokio::test]
async fn a_batch_that_overflows_a_listener_queue_fails_the_notifying_transaction() {
    let engine = SqlEngine::new();
    let (mut listener, mut rx) = connect(&engine, 11);
    let (mut notifier, _notifier_rx) = connect(&engine, 22);
    tag(&mut listener, "LISTEN news").await;
    let capacity = u32::try_from(crabka_pgexec::notify::NOTIFY_QUEUE_CAPACITY).expect("capacity");

    // Exactly the queue capacity fits, and every notification is delivered.
    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query(&notify_batch(capacity))
        .await
        .expect("a batch that exactly fills the queue");
    tag(&mut notifier, "COMMIT").await;
    let delivered = drain(&mut rx);
    assert!(delivered.len() == capacity as usize);
    assert!(delivered[0] == notification(22, "news", "p0"));

    // One more than the queue holds fails the *notifying* transaction, and the
    // listener is neither disconnected nor sent a partial batch.
    tag(&mut notifier, "BEGIN").await;
    notifier
        .simple_query(&notify_batch(capacity + 1))
        .await
        .expect("queueing itself succeeds; the reservation happens at commit");
    let failure = error(&mut notifier, "COMMIT").await;
    assert!(failure.code == "54000");
    assert!(rx.try_recv() == Err(TryRecvError::Empty));

    // Both connections carry on afterwards.
    tag(&mut notifier, "NOTIFY news, 'after the overflow'").await;
    assert!(rx.try_recv() == Ok(notification(22, "news", "after the overflow")));
}
