//! `DROP TABLE` and `TRUNCATE` against a relation an open cursor of the same
//! session is still reading.
//!
//! `PostgreSQL` refuses both with 55006. The refusal comes from the relation's
//! reference count rather than from a lock, because the `ACCESS EXCLUSIVE` lock
//! the command takes keeps other sessions out but does not keep the session out
//! of its own portals. Every expectation below was read off `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error:?}"))
}

/// The `(SQLSTATE, message)` pair `sql` fails with.
async fn failure(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("`{sql}` should have failed"));
    (error.code.clone(), error.message.clone())
}

/// What `PostgreSQL` 18.4 answers for a `DROP TABLE`/`TRUNCATE` blocked by a
/// cursor of the same session. The relation is named bare, never qualified.
fn in_use(command: &str, relation: &str) -> (String, String) {
    (
        "55006".to_string(),
        format!(
            "cannot {command} \"{relation}\" because it is being used by \
             active queries in this session"
        ),
    )
}

async fn seeded() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE pinned (a int4);
         INSERT INTO pinned VALUES (1), (2), (3);
         CREATE TABLE other (a int4);
         INSERT INTO other VALUES (1)",
    )
    .await;
    (engine, session)
}

#[tokio::test]
async fn a_cursor_blocks_both_commands_against_the_relation_it_reads() {
    for (command, sql) in [
        ("DROP TABLE", "DROP TABLE pinned"),
        ("DROP TABLE", "DROP TABLE public.pinned CASCADE"),
        ("TRUNCATE", "TRUNCATE pinned"),
        ("TRUNCATE", "TRUNCATE TABLE ONLY public.pinned"),
        ("TRUNCATE", "TRUNCATE other, pinned"),
    ] {
        let (_engine, mut session) = seeded().await;
        run(
            &mut session,
            "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c",
        )
        .await;
        assert!(
            failure(&mut session, sql).await == in_use(command, "pinned"),
            "{sql}"
        );
    }
}

/// A `FETCH` that ran off the end, and a cursor never fetched from at all, both
/// still hold the relation. Only `CLOSE` and the end of the transaction let go.
#[tokio::test]
async fn how_far_the_cursor_has_read_does_not_matter() {
    for progress in ["", "FETCH ALL FROM c", "FETCH ALL FROM c; FETCH ALL FROM c"] {
        let (_engine, mut session) = seeded().await;
        run(
            &mut session,
            "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned",
        )
        .await;
        if !progress.is_empty() {
            run(&mut session, progress).await;
        }
        assert!(
            failure(&mut session, "DROP TABLE pinned").await == in_use("DROP TABLE", "pinned"),
            "after `{progress}`"
        );
    }
}

#[tokio::test]
async fn closing_the_cursor_releases_the_relation() {
    let (_engine, mut session) = seeded().await;
    run(
        &mut session,
        "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c; CLOSE c",
    )
    .await;
    run(&mut session, "DROP TABLE pinned").await;
}

#[tokio::test]
async fn a_cursor_over_one_relation_leaves_the_others_alone() {
    let (_engine, mut session) = seeded().await;
    run(
        &mut session,
        "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c",
    )
    .await;
    run(&mut session, "TRUNCATE other").await;
}

/// A `WITH HOLD` cursor holds the relation for as long as its declaring
/// transaction runs. That transaction's `COMMIT` copies the rows out, and the
/// relation is free from then on.
#[tokio::test]
async fn a_holdable_cursor_releases_the_relation_at_the_commit_it_survives() {
    let (_engine, mut session) = seeded().await;
    run(
        &mut session,
        "BEGIN; DECLARE h CURSOR WITH HOLD FOR SELECT * FROM pinned; FETCH 1 FROM h",
    )
    .await;
    assert!(failure(&mut session, "TRUNCATE pinned").await == in_use("TRUNCATE", "pinned"));
    run(&mut session, "ROLLBACK").await;

    run(
        &mut session,
        "BEGIN; DECLARE h CURSOR WITH HOLD FOR SELECT * FROM pinned; FETCH 1 FROM h; COMMIT",
    )
    .await;
    run(&mut session, "TRUNCATE pinned").await;
}

/// The relation does not have to be the query's first `FROM` item, and a `WITH`
/// name that shadows a real relation is a query and not that relation.
#[tokio::test]
async fn the_whole_query_tree_is_searched_and_a_cte_name_is_not_a_relation() {
    let cases: &[(&str, bool)] = &[
        (
            "SELECT * FROM other WHERE EXISTS (SELECT 1 FROM pinned)",
            true,
        ),
        ("SELECT (SELECT count(*) FROM pinned) FROM other", true),
        ("SELECT * FROM other JOIN pinned USING (a)", true),
        ("SELECT * FROM (SELECT * FROM pinned) s", true),
        ("SELECT * FROM other UNION ALL SELECT * FROM pinned", true),
        ("WITH pinned AS (SELECT 1 AS a) SELECT * FROM pinned", false),
        ("SELECT * FROM other", false),
    ];
    for (query, blocked) in cases {
        let (_engine, mut session) = seeded().await;
        run(&mut session, "BEGIN").await;
        run(&mut session, &format!("DECLARE c CURSOR FOR {query}")).await;
        assert!(
            session.simple_query("TRUNCATE pinned").await.is_err() == *blocked,
            "{query}"
        );
    }
}

/// A cursor is transaction state, so the transaction that declared it takes it
/// away — and so does the rollback of the sub-transaction that declared it.
#[tokio::test]
async fn a_cursor_the_session_no_longer_holds_stops_blocking() {
    for teardown in ["ROLLBACK", "COMMIT", "ROLLBACK TO SAVEPOINT s"] {
        let (_engine, mut session) = seeded().await;
        run(&mut session, "BEGIN; SAVEPOINT s").await;
        run(
            &mut session,
            "DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c",
        )
        .await;
        assert!(
            failure(&mut session, "TRUNCATE pinned").await == in_use("TRUNCATE", "pinned"),
            "{teardown}"
        );
        // The refusal left the block aborted, so the cursor is re-declared in a
        // fresh one and the teardown under test is what removes it this time.
        run(&mut session, "ROLLBACK; BEGIN; SAVEPOINT s").await;
        run(
            &mut session,
            "DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c",
        )
        .await;
        run(&mut session, teardown).await;
        run(&mut session, "TRUNCATE pinned").await;
    }
}

/// The refusal is this session's own business. Another session waits for the
/// lock rather than being refused, so its cursors never enter the question.
#[tokio::test]
async fn a_cursor_in_another_session_is_not_this_session_s_concern() {
    let (engine, mut reader) = seeded().await;
    let mut writer = engine.connect();
    run(
        &mut reader,
        "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned; FETCH 1 FROM c",
    )
    .await;
    run(&mut writer, "TRUNCATE pinned").await;
}

/// A cursor over a view holds the tables under the view and not the view
/// itself: 18.4 drops such a view without complaint, because the rewriter
/// replaced it before the portal opened.
#[tokio::test]
async fn a_view_a_cursor_reads_is_still_droppable() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "CREATE VIEW pinned_v AS SELECT * FROM pinned").await;
    run(
        &mut session,
        "BEGIN; DECLARE c CURSOR FOR SELECT * FROM pinned_v; FETCH 1 FROM c",
    )
    .await;
    run(&mut session, "DROP VIEW pinned_v").await;
}

/// `ON COMMIT DELETE ROWS` empties its table through the same write path a user
/// `TRUNCATE` takes. The commit that empties it is also the commit that closes
/// the cursor, so the emptying must not be refused.
#[tokio::test]
async fn on_commit_delete_rows_is_not_a_user_truncate() {
    let (_engine, mut session) = seeded().await;
    run(
        &mut session,
        "CREATE TEMP TABLE oncommit (a int4) ON COMMIT DELETE ROWS",
    )
    .await;
    run(
        &mut session,
        "BEGIN;
         INSERT INTO oncommit VALUES (1);
         DECLARE c CURSOR FOR SELECT * FROM oncommit;
         FETCH 1 FROM c;
         COMMIT",
    )
    .await;
    let results = run(&mut session, "SELECT count(*) FROM oncommit").await;
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("count returns rows: {:?}", results[0]);
    };
    let counted = rows[0][0]
        .as_ref()
        .map(|cell| String::from_utf8(cell.text.to_vec()).expect("server text is UTF-8"));
    assert!(counted == Some("0".to_string()));
}
