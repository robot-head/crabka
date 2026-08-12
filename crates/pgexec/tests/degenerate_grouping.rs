//! A *degenerate* grouping query — `HAVING` and/or grouping sets with no
//! aggregate and no `GROUP BY` — has exactly one group whatever its input is,
//! so `PostgreSQL` answers it without reading the FROM clause at all. These
//! tests pin which queries earn that and, more importantly, which do not: a
//! query that stops scanning when it should have scanned returns a wrong answer
//! instead of an error.
//!
//! The table every test seeds holds a zero, so `WHERE 1/a = 1` divides by zero
//! on the first row that reaches it. Whether the error appears is therefore a
//! direct read of whether the input was scanned.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn client() -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
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
    client
        .batch_execute("CREATE TABLE test_having (a int4, b text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO test_having VALUES (0, 'x'), (1, 'y'), (2, 'z')")
        .await
        .expect("insert");
    client
}

/// The `int4` values one query answered, or its SQLSTATE.
async fn ones(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<i32>, String> {
    match client.query(sql, &[]).await {
        Ok(rows) => Ok(rows.iter().map(|row| row.get(0)).collect()),
        Err(error) => Err(error
            .as_db_error()
            .expect("a database error")
            .code()
            .code()
            .to_string()),
    }
}

/// `select_having`'s last query, and the two before it that already stand for
/// the empty and the satisfied `HAVING`.
#[tokio::test]
async fn a_variable_free_having_answers_without_scanning() {
    let client = client().await;
    assert!(ones(&client, "SELECT 1 AS one FROM test_having HAVING 1 > 2").await == Ok(vec![]));
    assert!(ones(&client, "SELECT 1 AS one FROM test_having HAVING 1 < 2").await == Ok(vec![1]));
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 HAVING 1 < 2"
        )
        .await
            == Ok(vec![1])
    );
}

/// The rule is not "`HAVING` is constant". A `HAVING` that has to be evaluated
/// is still degenerate as long as it cannot read the input, and it is evaluated
/// exactly once.
#[tokio::test]
async fn a_non_constant_having_is_still_degenerate() {
    let client = client().await;
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 HAVING random() <= 1"
        )
        .await
            == Ok(vec![1])
    );
    assert!(
        ones(
            &client,
            "SELECT length('abc') AS n FROM test_having WHERE 1/a = 1 HAVING length('ab') = 2"
        )
        .await
            == Ok(vec![3])
    );
}

/// An aggregate reads the input, so the input is read — and `WHERE` with it.
#[tokio::test]
async fn an_aggregate_keeps_the_scan() {
    let client = client().await;
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 HAVING count(*) >= 0"
        )
        .await
            == Err("22012".to_string())
    );
    assert!(
        ones(
            &client,
            "SELECT count(*)::int4 AS n FROM test_having WHERE 1/a = 1 HAVING 1 < 2"
        )
        .await
            == Err("22012".to_string())
    );
    // Without the WHERE, that same query still counts every row rather than
    // aggregating over an empty input.
    assert!(
        ones(
            &client,
            "SELECT count(*)::int4 AS n FROM test_having HAVING 1 < 2"
        )
        .await
            == Ok(vec![3])
    );
}

/// `GROUP BY` makes the number of output rows depend on the input, so the input
/// is read.
#[tokio::test]
async fn a_group_by_keeps_the_scan() {
    let client = client().await;
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 GROUP BY a HAVING 1 < 2"
        )
        .await
            == Err("22012".to_string())
    );
    // The grouping keys still come from the rows, not from nowhere.
    assert!(
        ones(
            &client,
            "SELECT a FROM test_having GROUP BY a HAVING 1 < 2 ORDER BY a"
        )
        .await
            == Ok(vec![0, 1, 2])
    );
}

/// Every error the FROM clause and the clauses above it can raise is still
/// raised: the relation is resolved, and an ungrouped column is still 42803
/// rather than a silently NULL answer.
#[tokio::test]
async fn resolution_errors_survive_the_elision() {
    let client = client().await;
    assert!(
        ones(&client, "SELECT 1 AS one FROM test_having HAVING a > 1").await
            == Err("42803".to_string())
    );
    assert!(
        ones(&client, "SELECT a FROM test_having HAVING min(a) < max(a)").await
            == Err("42803".to_string())
    );
    assert!(
        ones(&client, "SELECT 1 AS one FROM no_such_table HAVING 1 < 2").await
            == Err("42P01".to_string())
    );
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE no_such_column = 1 HAVING 1 < 2"
        )
        .await
            == Err("42703".to_string())
    );
}

/// The empty grouping set is degenerate for the same reason `HAVING` is, and
/// each one emits its row.
#[tokio::test]
async fn empty_grouping_sets_are_degenerate() {
    let client = client().await;
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 GROUP BY ()"
        )
        .await
            == Ok(vec![1])
    );
    assert!(
        ones(
            &client,
            "SELECT 1 AS one FROM test_having WHERE 1/a = 1 GROUP BY GROUPING SETS ((), ())"
        )
        .await
            == Ok(vec![1, 1])
    );
}
