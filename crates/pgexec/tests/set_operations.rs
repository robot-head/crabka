//! SP38: UNION / INTERSECT / EXCEPT [ALL] — end-to-end over the wire (simple query
//! protocol → exercises the engine's own execution + text encoding), complementing
//! the in-crate unit tests in `crabka_pgexec::setops`.

use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
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

/// All first-column text values of a simple query's row results.
async fn col0(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(row.get(0).map(std::string::ToString::to_string));
        }
    }
    out
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err("expected error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn union_intersect_except_over_the_wire() {
    let port = spawn().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE t (a int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1),(2),(2),(3)")
        .await
        .expect("seed t");
    c.simple_query("CREATE TABLE u (a int4)")
        .await
        .expect("create u");
    c.simple_query("INSERT INTO u VALUES (2),(3),(4)")
        .await
        .expect("seed u");

    // UNION dedups + ORDER BY sorts.
    assert_eq!(
        col0(&c, "SELECT a FROM t UNION SELECT a FROM u ORDER BY a").await,
        vec![
            Some("1".into()),
            Some("2".into()),
            Some("3".into()),
            Some("4".into())
        ]
    );
    // UNION ALL keeps duplicates; ORDER BY a => the full multiset sorted.
    // t = {1,2,2,3}, u = {2,3,4} => [1,2,2,2,3,3,4]
    assert_eq!(
        col0(&c, "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a").await,
        vec![
            Some("1".into()),
            Some("2".into()),
            Some("2".into()),
            Some("2".into()),
            Some("3".into()),
            Some("3".into()),
            Some("4".into())
        ]
    );
    // INTERSECT distinct => {2,3}
    assert_eq!(
        col0(&c, "SELECT a FROM t INTERSECT SELECT a FROM u ORDER BY a").await,
        vec![Some("2".into()), Some("3".into())]
    );
    // EXCEPT distinct => {1} (values in t not in u)
    assert_eq!(
        col0(&c, "SELECT a FROM t EXCEPT SELECT a FROM u ORDER BY a").await,
        vec![Some("1".into())]
    );
}

#[tokio::test]
async fn set_op_type_unification_and_paren_topn() {
    let port = spawn().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE t (a int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1),(2),(3)")
        .await
        .expect("seed t");

    // int4 ∪ int8 → int8 column; first-branch name `x` wins; value round-trips.
    assert_eq!(
        col0(
            &c,
            "SELECT a AS x FROM t UNION SELECT 9999999999 ORDER BY x"
        )
        .await,
        vec![
            Some("1".into()),
            Some("2".into()),
            Some("3".into()),
            Some("9999999999".into())
        ]
    );

    // result-level LIMIT/OFFSET over the combined output.
    assert_eq!(
        col0(
            &c,
            "SELECT a FROM t UNION SELECT 10 ORDER BY a LIMIT 2 OFFSET 1"
        )
        .await,
        vec![Some("2".into()), Some("3".into())]
    );

    // top-N per parenthesized branch.
    assert_eq!(
        col0(&c, "(SELECT a FROM t ORDER BY a LIMIT 1) UNION (SELECT a FROM t ORDER BY a DESC LIMIT 1) ORDER BY a").await,
        vec![Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn set_op_error_surface() {
    let port = spawn().await;
    let c = connect(port).await;
    // column-count mismatch => 42601
    assert_eq!(err_code(&c, "SELECT 1 UNION SELECT 1, 2").await, "42601");
    // two CONCRETE incompatible types => 42804. An explicit `::text` makes the
    // literal a real text value (not the `unknown` pseudo-type), so it clashes with
    // int4 at type-resolution time — exactly PG's `select_common_type` 42804.
    assert_eq!(
        err_code(&c, "SELECT 1 UNION SELECT 'x'::text").await,
        "42804"
    );
    // a BARE 'x' literal is `unknown`: it resolves to the other branch's int4, then
    // the value fails the text→int4 parse => 22P02 (PG-faithful, see the
    // unknown-literal test below).
    assert_eq!(err_code(&c, "SELECT 1 UNION SELECT 'x'").await, "22P02");
    // out-of-range positional ORDER BY => 42P10
    assert_eq!(
        err_code(&c, "SELECT 1 UNION SELECT 2 ORDER BY 5").await,
        "42P10"
    );
}

/// `PostgreSQL` `unknown`-literal resolution across set-op branches: a bare `NULL` or
/// string literal takes the other branch's type and is coerced via the cast matrix
/// (a well-formed value parses; a bad one raises 22P02). All confirmed against a live
/// `PostgreSQL` 18 oracle.
#[tokio::test]
async fn unknown_literal_branches_resolve_like_pg() {
    let port = spawn().await;
    let c = connect(port).await;

    // bare NULL unifies to int4 and yields the NULL row (NOT a 42804 type clash).
    assert_eq!(
        col0(&c, "SELECT NULL UNION SELECT 2 ORDER BY 1").await,
        vec![Some("2".into()), None]
    );
    // a well-formed string literal resolves to int4 and parses.
    assert_eq!(
        col0(&c, "SELECT 1 UNION SELECT '5' ORDER BY 1").await,
        vec![Some("1".into()), Some("5".into())]
    );
    // both branches unknown → text (PG's final unknown→text rule).
    assert_eq!(
        col0(&c, "SELECT 'a' UNION SELECT 'b' ORDER BY 1").await,
        vec![Some("a".into()), Some("b".into())]
    );
    // a string literal resolves to a non-text type and parses (numeric here).
    assert_eq!(
        col0(&c, "SELECT 1.5 UNION SELECT '2' ORDER BY 1").await,
        vec![Some("1.5".into()), Some("2".into())]
    );
}
