//! SP34: uncorrelated subquery expressions — scalar `(SELECT …)`, `x [NOT] IN
//! (SELECT …)`, `[NOT] EXISTS (…)`, and `x op ANY|SOME|ALL (…)` — end-to-end over
//! the wire (simple query protocol → exercises the engine's own execution + text
//! encoding), plus the 21000 / 42601 error surface.

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

async fn seed(client: &tokio_postgres::Client) {
    client
        .simple_query("CREATE TABLE t (id int4, v int4)")
        .await
        .expect("create t");
    client
        .simple_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed t");
    client
        .simple_query("CREATE TABLE u (k int4)")
        .await
        .expect("create u");
    client
        .simple_query("INSERT INTO u VALUES (1), (3)")
        .await
        .expect("seed u");
}

#[tokio::test]
async fn scalar_subquery_projection_and_where() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(&client, "SELECT (SELECT max(v) FROM t)").await,
        vec![Some("30".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY id"
        )
        .await,
        vec![Some("3".into())]
    );
    // Zero rows → NULL.
    assert_eq!(
        col0(&client, "SELECT (SELECT v FROM t WHERE id = 99)").await,
        vec![None]
    );
}

#[tokio::test]
async fn in_not_in_exists_quantified() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id IN (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("2".into())]
    );
    assert_eq!(
        col0(&client, "SELECT EXISTS (SELECT 1 FROM u WHERE k = 3)").await,
        vec![Some("t".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE k = 99) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    // v > ALL (1,3) → all rows (10,20,30 each > 3); SOME synonym for ANY.
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE v > ALL (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id = SOME (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn correlated_exists_uses_each_outer_row_and_inner_names_shadow_it() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE EXISTS (SELECT 1 FROM u i WHERE i.k = o.id) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE NOT EXISTS (SELECT 1 FROM u i WHERE i.k = o.id) ORDER BY o.id"
        )
        .await,
        vec![Some("2".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE EXISTS (SELECT 1 FROM u i WHERE i.k = id) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE EXISTS (SELECT 1 FROM t i WHERE id = 3) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM public.t WHERE t.id = 3) ORDER BY t.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE EXISTS (SELECT 1 FROM u i WHERE i.k = o.id) \
             ORDER BY o.id FOR UPDATE"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn correlated_in_uses_each_outer_row() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE o.id IN (SELECT i.k FROM u i WHERE i.k <= o.id) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn correlated_subqueries_preserve_case_and_function_semantics() {
    let client = connect(spawn().await).await;
    seed(&client).await;

    // The division-by-zero subquery is in a dead CASE branch for id = 1.
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE CASE WHEN o.id = 1 THEN true \
             ELSE EXISTS (SELECT 1 WHERE 1 / (o.id - 1) = 1) END ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into())]
    );

    client
        .simple_query(
            "CREATE FUNCTION has_type(int4) RETURNS bool LANGUAGE SQL AS $$ SELECT true $$; \
             CREATE FUNCTION has_type(text) RETURNS bool LANGUAGE SQL AS $$ SELECT false $$",
        )
        .await
        .expect("create overloaded SQL functions");
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o WHERE has_type((SELECT o.id)) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE CASE WHEN EXISTS (SELECT 1 WHERE o.id = 1) \
                        THEN has_type(1) ELSE false END ORDER BY o.id"
        )
        .await,
        vec![Some("1".into())]
    );
    // COALESCE must not execute the correlated division-by-zero second argument.
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE coalesce(o.id = 1, \
                 EXISTS (SELECT 1 WHERE 1 / (o.id - 1) = 1)) ORDER BY o.id"
        )
        .await,
        vec![Some("1".into())]
    );

    // An uncorrelated subquery in the selected branch is still an initplan: it
    // runs once for the statement, not once for every outer row.
    client
        .simple_query("CREATE SEQUENCE correlated_initplan_seq")
        .await
        .expect("create initplan sequence");
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE CASE WHEN o.id > 0 \
                        THEN (SELECT nextval('correlated_initplan_seq')) > 0 \
                        ELSE EXISTS (SELECT 1 WHERE o.id = 0) END \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(&client, "SELECT nextval('correlated_initplan_seq')").await,
        vec![Some("2".into())]
    );

    // The same once-only rule applies to the uncorrelated LHS sibling of a
    // directly correlated IN-subquery node.
    client
        .simple_query("CREATE SEQUENCE correlated_in_lhs_seq")
        .await
        .expect("create IN lhs sequence");
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE (SELECT nextval('correlated_in_lhs_seq')) IN (SELECT o.id) \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into())]
    );
    assert_eq!(
        col0(&client, "SELECT nextval('correlated_in_lhs_seq')").await,
        vec![Some("2".into())]
    );

    // Conversely, an uncorrelated subquery in a dead branch stays lazy.
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE CASE WHEN EXISTS (SELECT 1 WHERE o.id > 0) THEN true \
                        ELSE (SELECT 1 / 0) = 1 END \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );

    // Lazy expressions nested below the correlated CASE still recognize a
    // deferred initplan marker and do not initialize an unused argument/branch.
    client
        .simple_query("CREATE SEQUENCE correlated_dead_initplan_seq")
        .await
        .expect("create dead initplan sequence");
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE CASE WHEN EXISTS (SELECT 1 WHERE o.id > 0) \
                        THEN coalesce(true, \
                             (SELECT nextval('correlated_dead_initplan_seq')) > 0) \
                        ELSE false END \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(&client, "SELECT nextval('correlated_dead_initplan_seq')").await,
        vec![Some("1".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE CASE WHEN EXISTS (SELECT 1 WHERE o.id > 0) \
                        THEN CASE WHEN true THEN true ELSE (SELECT 1 / 0) = 1 END \
                        ELSE false END \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn correlated_binding_tracks_ctes_windows_and_exact_qualifiers() {
    let client = connect(spawn().await).await;
    seed(&client).await;

    // The local CTE supplies only x, so id falls back to the outer row.
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE EXISTS (WITH c AS (SELECT 1 AS x) \
                           SELECT 1 FROM c WHERE id = 1) \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into())]
    );

    // Outer references in lifted window-call metadata are correlated too.
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE EXISTS (SELECT row_number() OVER (ORDER BY o.id) FROM u i) \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );

    // Quoted identifiers are case-sensitive: inner t does not shadow outer T.
    assert_eq!(
        col0(
            &client,
            "SELECT \"T\".id FROM t AS \"T\" \
             WHERE EXISTS (SELECT 1 FROM t t WHERE \"T\".id = 1)"
        )
        .await,
        vec![Some("1".into())]
    );

    // An ambiguous bindable outer name keeps 42702 instead of falling through
    // to the inner resolver as an undefined column.
    assert_eq!(
        err_code(
            &client,
            "SELECT a.id FROM t a, t b WHERE EXISTS (SELECT 1 WHERE id = 1)"
        )
        .await,
        "42702"
    );
}

#[tokio::test]
async fn inner_order_by_labels_shadow_outer_columns() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    client
        .simple_query(
            "CREATE TABLE reverse_u (k int4); INSERT INTO reverse_u VALUES (3), (1); \
             CREATE TABLE outer_abs (abs int4); INSERT INTO outer_abs VALUES (100)",
        )
        .await
        .expect("seed reverse order");
    assert_eq!(
        col0(
            &client,
            "SELECT o.id FROM t o \
             WHERE (SELECT k AS id FROM reverse_u ORDER BY id LIMIT 1) = 1 \
             ORDER BY o.id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT o.abs FROM outer_abs o \
             WHERE (SELECT abs(k) FROM reverse_u ORDER BY abs LIMIT 1) = 1"
        )
        .await,
        vec![Some("100".into())]
    );
}

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // scalar subquery > 1 row → 21000.
    assert_eq!(err_code(&client, "SELECT (SELECT v FROM t)").await, "21000");
    // scalar subquery > 1 column → 42601.
    assert_eq!(
        err_code(&client, "SELECT (SELECT id, v FROM t WHERE id = 1)").await,
        "42601"
    );
    // IN-subquery > 1 column → 42601.
    assert_eq!(
        err_code(
            &client,
            "SELECT id FROM t WHERE id IN (SELECT id, v FROM t)"
        )
        .await,
        "42601"
    );
    client
        .simple_query("CREATE TABLE empty_outer (id int4)")
        .await
        .expect("create empty outer table");
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE EXISTS (SELECT 1 FROM no_such_relation WHERE o.id = 1)"
        )
        .await,
        "42P01"
    );
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE o.id IN (SELECT o.id, o.id)"
        )
        .await,
        "42601"
    );
    // Correlation on the IN RHS must not suppress validation of its LHS.
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE (SELECT o.id, o.id) IN (SELECT o.id)"
        )
        .await,
        "42601"
    );
    // The same error cannot be deferred as a lazy uncorrelated initplan just
    // because the malformed scalar expression is beside a correlated RHS.
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE (SELECT 1, 2) IN (SELECT o.id)"
        )
        .await,
        "42601"
    );
    // Predicate typing happens even when the outer relation has no rows.
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE CASE WHEN EXISTS (SELECT 1 WHERE o.id = 1) THEN 1 ELSE 2 END"
        )
        .await,
        "42804"
    );
    assert_eq!(
        err_code(
            &client,
            "SELECT o.id FROM empty_outer o \
             WHERE coalesce(EXISTS (SELECT 1 WHERE o.id = 1), 1)"
        )
        .await,
        "42804"
    );
}

/// A subquery in FROM may omit its alias, as it has since `PostgreSQL` 16. Several
/// unnamed subqueries can appear in one FROM without colliding, and the columns
/// they expose are usable unqualified.
#[tokio::test]
async fn a_from_subquery_may_omit_its_alias() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    assert!(col0(&client, "SELECT * FROM (SELECT 1 AS x)").await == vec![Some("1".to_string())]);
    assert!(col0(&client, "SELECT count(*) FROM (SELECT 1)").await == vec![Some("1".to_string())]);
    // The exposed column is in scope unqualified.
    assert!(
        col0(&client, "SELECT x FROM (SELECT 1 AS x) WHERE x = 1").await
            == vec![Some("1".to_string())]
    );
    // Two of them in one FROM each get their own name rather than clashing.
    assert!(
        col0(&client, "SELECT x FROM (SELECT 1 AS x), (SELECT 2 AS y)").await
            == vec![Some("1".to_string())]
    );
    // An alias, when given, still wins.
    assert!(
        col0(&client, "SELECT q.x FROM (SELECT 1 AS x) q").await == vec![Some("1".to_string())]
    );
}
