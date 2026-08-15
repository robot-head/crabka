//! `pg_snapshot`, `txid_snapshot` and the transaction-id functions over both,
//! end to end over the wire.
//!
//! What this file pins is the part that no value type can supply on its own:
//! the types reach SQL, a snapshot survives a table, and the functions export
//! the engine's *own* transaction state rather than describing one. The last
//! point is what the tests here work hardest at — an id
//! `pg_current_xact_id()` hands out has to be a real running transaction that
//! later commits or aborts, and the transaction it names has to be the one the
//! session is actually in.
//!
//! Every expectation was taken from the pinned `PostgreSQL` 18.4 build.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn spawn() -> u16 {
    spawn_with_engine().await.0
}

/// The port, and the engine behind it — for the one test that has to run
/// maintenance, which has no SQL spelling here.
async fn spawn_with_engine() -> (u16, Arc<SqlEngine>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let engine = Arc::new(SqlEngine::new());
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::clone(&engine),
        Arc::new(SessionConfig::trust()),
    ));
    (port, engine)
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

/// The first column of every row, as the engine's own text encoding.
async fn column(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    client
        .simple_query(sql)
        .await
        .expect(sql)
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_string)),
            _ => None,
        })
        .collect()
}

/// The first column of the first row, which must not be NULL.
async fn text(client: &tokio_postgres::Client, sql: &str) -> String {
    column(client, sql)
        .await
        .first()
        .cloned()
        .flatten()
        .unwrap_or_else(|| panic!("null or absent first column for `{sql}`"))
}

/// The first column of the first row, NULL included.
async fn maybe_text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    column(client, sql)
        .await
        .first()
        .cloned()
        .unwrap_or_else(|| panic!("no row for `{sql}`"))
}

/// The first column of the first row as a `u64`.
async fn number(client: &tokio_postgres::Client, sql: &str) -> u64 {
    text(client, sql)
        .await
        .parse()
        .unwrap_or_else(|_| panic!("not a number from `{sql}`"))
}

/// `(SQLSTATE, message)` of the error `sql` raises.
async fn error(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = client
        .simple_query(sql)
        .await
        .expect_err(sql)
        .as_db_error()
        .expect("db error")
        .clone();
    (error.code().code().to_string(), error.message().to_string())
}

async fn run(client: &tokio_postgres::Client, sql: &str) {
    client.simple_query(sql).await.expect(sql);
}

/// Both names resolve, both report their own oid, and both print the value the
/// same way — because both run `pg_snapshot_out`.
#[tokio::test]
async fn both_snapshot_types_resolve_and_share_one_value() {
    let client = connect(spawn().await).await;
    for name in ["pg_snapshot", "txid_snapshot"] {
        assert!(text(&client, &format!("SELECT '12:13:'::{name}")).await == "12:13:");
        assert!(
            text(&client, &format!("SELECT '12:18:14,16'::{name}")).await == "12:18:14,16",
            "{name}"
        );
        // A repeated id is one running transaction, so it folds.
        assert!(text(&client, &format!("SELECT '12:16:14,14'::{name}")).await == "12:16:14");
        assert!(text(&client, &format!("SELECT pg_typeof('12:13:'::{name})")).await == name);
    }
    assert!(text(&client, "SELECT '12:13:'::pg_snapshot::text").await == "12:13:");
}

/// Every rejection is 22P02 and names `pg_snapshot`, even where
/// `txid_snapshot` was written — `txid_snapshot_in` *is* `pg_snapshot_in`.
#[tokio::test]
async fn a_rejected_snapshot_is_22p02_naming_pg_snapshot() {
    let client = connect(spawn().await).await;
    for name in ["pg_snapshot", "txid_snapshot"] {
        for bad in ["31:12:", "0:1:", "12:13:0", "12:16:14,13"] {
            let raised = error(&client, &format!("SELECT '{bad}'::{name}")).await;
            assert!(
                raised
                    == (
                        "22P02".to_string(),
                        format!("invalid input syntax for type pg_snapshot: \"{bad}\"")
                    ),
                "{name} {bad}"
            );
        }
    }
}

/// A snapshot survives a table: the row encoding stores it and reads it back
/// as the same triple, including the wide running set.
#[tokio::test]
async fn a_snapshot_column_round_trips_through_storage() {
    let wide = "100:150:101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,\
                118,119,120,121,122,123,124,125,126,127,128,129,130,131";
    let client = connect(spawn().await).await;
    run(&client, "CREATE TABLE t (nr int, snap pg_snapshot)").await;
    run(&client, "CREATE TABLE u (nr int, snap txid_snapshot)").await;
    for (nr, value) in [
        (1, "12:13:"),
        (2, "12:20:13,15,18"),
        (3, "100001:100009:100005,100007,100008"),
        (4, wide),
    ] {
        run(&client, &format!("INSERT INTO t VALUES ({nr}, '{value}')")).await;
        run(&client, &format!("INSERT INTO u VALUES ({nr}, '{value}')")).await;
    }
    let stored: Vec<Option<String>> = column(&client, "SELECT snap FROM t ORDER BY nr").await;
    assert!(
        stored
            == [
                Some("12:13:".to_string()),
                Some("12:20:13,15,18".to_string()),
                Some("100001:100009:100005,100007,100008".to_string()),
                Some(wide.to_string()),
            ]
    );
    assert!(column(&client, "SELECT snap FROM u ORDER BY nr").await == stored);
    // The column keeps the type it was declared with, which is the whole
    // reason `txid_snapshot` is a type here and not an alias.
    assert!(text(&client, "SELECT pg_typeof(snap) FROM t LIMIT 1").await == "pg_snapshot");
    assert!(text(&client, "SELECT pg_typeof(snap) FROM u LIMIT 1").await == "txid_snapshot");
}

/// The accessors read the triple, and `xip` expands to one row per running id
/// in a select list beside the two scalars.
#[tokio::test]
async fn the_accessors_read_the_triple() {
    let client = connect(spawn().await).await;
    for (prefix, ty) in [
        ("pg_snapshot", "pg_snapshot"),
        ("txid_snapshot", "txid_snapshot"),
    ] {
        let snapshot = format!("'12:20:13,15,18'::{ty}");
        assert!(text(&client, &format!("SELECT {prefix}_xmin({snapshot})")).await == "12");
        assert!(text(&client, &format!("SELECT {prefix}_xmax({snapshot})")).await == "20");
        assert!(
            column(&client, &format!("SELECT {prefix}_xip({snapshot})")).await
                == [
                    Some("13".to_string()),
                    Some("15".to_string()),
                    Some("18".to_string()),
                ],
            "{prefix}"
        );
        // An empty running set expands to no row at all, so the row that
        // carried it disappears from the result.
        assert!(column(&client, &format!("SELECT {prefix}_xip('12:13:'::{ty})")).await == []);
    }
    // The lockstep expansion PostgreSQL 10+ gives a select list: the two
    // scalars repeat beside each running id.
    let rows = client
        .simple_query(
            "SELECT pg_snapshot_xmin(s), pg_snapshot_xmax(s), pg_snapshot_xip(s) \
             FROM (SELECT '12:20:13,15,18'::pg_snapshot AS s) q",
        )
        .await
        .expect("expansion");
    let cells: Vec<Vec<Option<String>>> = rows
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => {
                Some((0..3).map(|i| row.get(i).map(str::to_string)).collect())
            }
            _ => None,
        })
        .collect();
    assert!(
        cells
            == [
                vec![
                    Some("12".to_string()),
                    Some("20".to_string()),
                    Some("13".to_string())
                ],
                vec![
                    Some("12".to_string()),
                    Some("20".to_string()),
                    Some("15".to_string())
                ],
                vec![
                    Some("12".to_string()),
                    Some("20".to_string()),
                    Some("18".to_string())
                ],
            ]
    );
}

/// Visibility follows the window and then the running set, and the answer does
/// not depend on which family asked.
#[tokio::test]
async fn visibility_follows_the_window_then_the_running_set() {
    let client = connect(spawn().await).await;
    let expected = [
        (11, "t"),
        (12, "t"),
        (13, "f"),
        (14, "t"),
        (15, "f"),
        (18, "f"),
        (19, "t"),
        (20, "f"),
        (21, "f"),
    ];
    for (id, visible) in expected {
        assert!(
            text(
                &client,
                &format!(
                    "SELECT pg_visible_in_snapshot('{id}'::xid8, '12:20:13,15,18'::pg_snapshot)"
                )
            )
            .await
                == visible,
            "pg_visible_in_snapshot {id}"
        );
        // The deprecated spelling takes a `bigint`, so an `integer` column
        // widens into it the way `pg_cast` says it does.
        assert!(
            text(
                &client,
                &format!("SELECT txid_visible_in_snapshot({id}, '12:20:13,15,18'::txid_snapshot)")
            )
            .await
                == visible,
            "txid_visible_in_snapshot {id}"
        );
    }
}

/// The two families do not mix: each accessor takes its own snapshot type, as
/// `pg_proc` declares it.
#[tokio::test]
async fn an_accessor_refuses_the_other_familys_snapshot() {
    let client = connect(spawn().await).await;
    let cases = [
        (
            "SELECT pg_snapshot_xmin('12:13:'::txid_snapshot)",
            "function pg_snapshot_xmin(txid_snapshot) does not exist",
        ),
        (
            "SELECT txid_snapshot_xmin('12:13:'::pg_snapshot)",
            "function txid_snapshot_xmin(pg_snapshot) does not exist",
        ),
        (
            "SELECT pg_visible_in_snapshot(1, '12:13:'::pg_snapshot)",
            "function pg_visible_in_snapshot(integer, pg_snapshot) does not exist",
        ),
    ];
    for (sql, message) in cases {
        assert!(
            error(&client, sql).await == ("42883".to_string(), message.to_string()),
            "{sql}"
        );
    }
}

/// The transaction's own id: absent until something asks for it, then stable
/// for the rest of the transaction, and the same under either spelling.
#[tokio::test]
async fn the_transactions_own_id_is_assigned_once_and_then_stable() {
    let client = connect(spawn().await).await;
    run(&client, "BEGIN").await;
    assert!(
        maybe_text(&client, "SELECT pg_current_xact_id_if_assigned()")
            .await
            .is_none()
    );
    assert!(
        maybe_text(&client, "SELECT txid_current_if_assigned()")
            .await
            .is_none()
    );
    let assigned = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(number(&client, "SELECT pg_current_xact_id()").await == assigned);
    assert!(number(&client, "SELECT txid_current()").await == assigned);
    assert!(number(&client, "SELECT pg_current_xact_id_if_assigned()").await == assigned);
    run(&client, "COMMIT").await;
    // A committed id is remembered, and the next transaction gets its own.
    assert!(
        text(
            &client,
            &format!("SELECT pg_xact_status('{assigned}'::xid8)")
        )
        .await
            == "committed"
    );
    run(&client, "BEGIN").await;
    assert!(number(&client, "SELECT pg_current_xact_id()").await > assigned);
    run(&client, "COMMIT").await;
}

/// A write in the same transaction adopts the id the function handed out
/// rather than allocating a second one.
#[tokio::test]
async fn a_write_after_the_function_keeps_the_same_id() {
    let client = connect(spawn().await).await;
    run(&client, "CREATE TABLE t (a int)").await;
    run(&client, "BEGIN").await;
    let assigned = number(&client, "SELECT pg_current_xact_id()").await;
    run(&client, "INSERT INTO t VALUES (1)").await;
    assert!(number(&client, "SELECT pg_current_xact_id()").await == assigned);
    run(&client, "COMMIT").await;
    assert!(text(&client, "SELECT count(*)::text FROM t").await == "1");
    assert!(text(&client, &format!("SELECT txid_status({assigned})")).await == "committed");
}

/// An autocommit write that calls the function stores the id of the very
/// transaction that wrote the row, and settles it once.
///
/// A second id allocated behind the first would leave one of them running for
/// the life of the process, so the row's `xmin` and the reported id have to be
/// the same number.
#[tokio::test]
async fn an_autocommit_write_stores_the_id_that_wrote_the_row() {
    let client = connect(spawn().await).await;
    run(&client, "CREATE TABLE t (a xid8)").await;
    run(&client, "INSERT INTO t VALUES (pg_current_xact_id())").await;
    let stored = number(&client, "SELECT a::text::int8 FROM t").await;
    assert!(
        text(&client, &format!("SELECT pg_xact_status('{stored}'::xid8)")).await == "committed"
    );
    // Nothing that statement allocated is still running.
    let xmin = number(&client, "SELECT pg_snapshot_xmin(pg_current_snapshot())").await;
    assert!(xmin > stored, "xmin {xmin} still held down by {stored}");
}

/// The id an autocommit statement assigns still ends: it is committed with the
/// implicit transaction that wrapped the statement, and it stops running.
///
/// This is the regression this design most needs. An id handed out during
/// expression evaluation and never adopted by a transaction would stay in the
/// running set for the life of the process, and hold every later snapshot's
/// `xmin` down behind it.
#[tokio::test]
async fn an_autocommit_id_is_settled_and_stops_running() {
    let client = connect(spawn().await).await;
    let first = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(text(&client, &format!("SELECT pg_xact_status('{first}'::xid8)")).await == "committed");
    // Two more, each its own statement and so its own transaction.
    let second = number(&client, "SELECT pg_current_xact_id()").await;
    let third = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(second > first && third > second);
    // The extended protocol takes its own path to the same statement, so it
    // has to settle its id too.
    let extended: String = client
        .query_one("SELECT pg_current_xact_id()::text", &[])
        .await
        .expect("extended protocol")
        .get(0);
    let extended: u64 = extended.parse().expect("an id");
    assert!(extended > third);
    // Nothing is running any more, so the window has moved past all of them.
    let xmin = number(&client, "SELECT pg_snapshot_xmin(pg_current_snapshot())").await;
    assert!(xmin > extended, "xmin {xmin} still held down by {extended}");
}

/// A rolled-back transaction's id is remembered as aborted, and the rollback
/// reaches an id the function alone assigned.
#[tokio::test]
async fn a_rolled_back_id_reports_aborted() {
    let client = connect(spawn().await).await;
    run(&client, "BEGIN").await;
    let rolled_back = number(&client, "SELECT txid_current()").await;
    run(&client, "ROLLBACK").await;
    assert!(text(&client, &format!("SELECT txid_status({rolled_back})")).await == "aborted");
    assert!(
        text(
            &client,
            &format!("SELECT pg_xact_status('{rolled_back}'::xid8)")
        )
        .await
            == "aborted"
    );
}

/// The status of the transaction asking is `in progress`, and an id the engine
/// has never handed out is 22023 rather than a guess.
#[tokio::test]
async fn a_running_id_is_in_progress_and_a_future_one_is_refused() {
    let client = connect(spawn().await).await;
    run(&client, "BEGIN").await;
    let running = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(
        text(
            &client,
            &format!("SELECT pg_xact_status('{running}'::xid8)")
        )
        .await
            == "in progress"
    );
    let future = running + 10_000;
    assert!(
        error(&client, &format!("SELECT pg_xact_status('{future}'::xid8)")).await
            == (
                "22023".to_string(),
                format!("transaction ID {future} is in the future")
            )
    );
    run(&client, "COMMIT").await;
}

/// `PostgreSQL` reserves three transaction ids, and `pg_xact_status` answers
/// for every one of them without reading the clog.
///
/// Id 2 is the one this can get wrong. The engine reserves only 0 and 1, so 2
/// is an ordinary id it hands to the first real transaction — old enough by
/// now that its clog entry has been truncated. Left to the clog path it reads
/// as "no longer known" and comes back NULL, where `TransactionLogFetch`
/// answers `FrozenTransactionId` "committed" before it looks anything up.
#[tokio::test]
async fn the_reserved_transaction_ids_answer_as_postgresql_does() {
    let client = connect(spawn().await).await;
    // `InvalidTransactionId` names no transaction, so it is NULL.
    // `BootstrapTransactionId` wrote the initial catalog and
    // `FrozenTransactionId` is every frozen row's creator: both committed.
    for (xid, expected) in [
        (0_u64, None),
        (1, Some("committed")),
        (2, Some("committed")),
    ] {
        let modern = maybe_text(&client, &format!("SELECT pg_xact_status('{xid}'::xid8)")).await;
        let legacy = maybe_text(&client, &format!("SELECT txid_status({xid})")).await;
        assert!(
            modern.as_deref() == expected,
            "pg_xact_status({xid}) was {modern:?}, expected {expected:?}"
        );
        assert!(
            legacy.as_deref() == expected,
            "txid_status({xid}) was {legacy:?}, expected {expected:?}"
        );
    }
}

/// The exported snapshot is the engine's own, so the two relations
/// `PostgreSQL`'s own suite asserts hold here too.
#[tokio::test]
async fn the_exported_snapshot_agrees_with_the_id_it_was_taken_beside() {
    let client = connect(spawn().await).await;
    for sql in [
        "SELECT pg_current_xact_id() >= pg_snapshot_xmin(pg_current_snapshot())",
        "SELECT txid_current() >= txid_snapshot_xmin(txid_current_snapshot())",
    ] {
        assert!(text(&client, sql).await == "t", "{sql}");
    }
    // The transaction asking cannot be visible to a snapshot taken before it
    // had an id, whichever side of the window it lands on.
    for sql in [
        "SELECT pg_visible_in_snapshot(pg_current_xact_id(), pg_current_snapshot())",
        "SELECT txid_visible_in_snapshot(txid_current(), txid_current_snapshot())",
    ] {
        assert!(text(&client, sql).await == "f", "{sql}");
    }
    // A snapshot taken inside a REPEATABLE READ block does not move under it.
    run(&client, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    let first = text(&client, "SELECT pg_current_snapshot()::text").await;
    assert!(text(&client, "SELECT pg_current_snapshot()::text").await == first);
    run(&client, "COMMIT").await;
}

/// An id whose clog entry vacuum has truncated is answered NULL, not
/// `aborted`.
///
/// This is `PostgreSQL`'s `oldestClogXid` test, and it is the difference
/// between "this transaction never committed" and "this engine no longer
/// knows". Both are the same absent key, so only the recorded truncation floor
/// tells them apart — and answering `aborted` would be a wrong answer for
/// every transaction that committed and was then forgotten.
#[tokio::test]
async fn a_truncated_id_is_answered_null_rather_than_aborted() {
    let (port, engine) = spawn_with_engine().await;
    let client = connect(port).await;
    run(&client, "CREATE TABLE t (a int)").await;
    run(&client, "INSERT INTO t VALUES (1)").await;
    let committed = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(
        text(
            &client,
            &format!("SELECT pg_xact_status('{committed}'::xid8)")
        )
        .await
            == "committed"
    );
    // Burn enough settled transactions that the horizon can move past the one
    // above, then sweep.
    for _ in 0..8 {
        run(&client, "INSERT INTO t VALUES (2)").await;
    }
    engine.vacuum().await.expect("vacuum");
    assert!(
        maybe_text(
            &client,
            &format!("SELECT pg_xact_status('{committed}'::xid8)")
        )
        .await
        .is_none(),
        "id {committed} was truncated and must not be reported aborted"
    );
    // An id the sweep did not reach still answers, so the floor is read and
    // not assumed.
    let recent = number(&client, "SELECT pg_current_xact_id()").await;
    assert!(
        text(&client, &format!("SELECT pg_xact_status('{recent}'::xid8)")).await == "committed"
    );
}

/// Every one of these is strict, so a NULL argument is answered without the
/// transaction state being consulted at all.
#[tokio::test]
async fn a_null_argument_answers_null() {
    let client = connect(spawn().await).await;
    for call in [
        "pg_snapshot_xmin(NULL)",
        "pg_snapshot_xmax(NULL)",
        "txid_snapshot_xmin(NULL)",
        "pg_visible_in_snapshot(NULL, '12:13:'::pg_snapshot)",
        "pg_visible_in_snapshot('1'::xid8, NULL)",
        "pg_xact_status(NULL)",
        "txid_status(NULL)",
    ] {
        assert!(
            maybe_text(&client, &format!("SELECT {call}"))
                .await
                .is_none(),
            "{call}"
        );
    }
    // A set-returning one produces no row rather than one NULL row.
    assert!(column(&client, "SELECT pg_snapshot_xip(NULL)").await == []);
}
