//! SP28: predicate and conditional expression breadth. IS [NOT] NULL, [NOT] IN,
//! [NOT] BETWEEN, [NOT] LIKE/ILIKE, CASE, SELECT DISTINCT and OFFSET, end-to-end
//! over the wire, with three-valued NULL semantics and the error SQLSTATEs.

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

/// `pr(id int4, name text, amount int4)` with NULLs in both `name` and `amount`.
async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE pr (id int4, name text, amount int4)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO pr VALUES \
             (1, 'apple', 10), (2, 'banana', NULL), (3, 'cherry', 30), \
             (4, NULL, 5), (5, 'avocado', 10)",
        )
        .await
        .expect("insert");
}

async fn ids(client: &tokio_postgres::Client, sql: &str) -> Vec<i32> {
    client
        .query(sql, &[])
        .await
        .expect("query")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect()
}

#[tokio::test]
async fn is_null_and_is_not_null() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        ids(&client, "SELECT id FROM pr WHERE amount IS NULL").await,
        [2]
    );
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE name IS NOT NULL ORDER BY id"
        )
        .await,
        [1, 2, 3, 5]
    );
}

#[tokio::test]
async fn in_list_and_not_in_with_null() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE amount IN (10, 30) ORDER BY id"
        )
        .await,
        [1, 3, 5]
    );
    // NOT IN excludes the NULL-amount row (unknown), keeping only definite misses.
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE amount NOT IN (10) ORDER BY id"
        )
        .await,
        [3, 4]
    );
    // A NULL in the list makes every non-matching row unknown -> no rows.
    assert_eq!(
        ids(&client, "SELECT id FROM pr WHERE amount NOT IN (10, NULL)").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn between_inclusive_bounds() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE amount BETWEEN 5 AND 10 ORDER BY id"
        )
        .await,
        [1, 4, 5]
    );
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE amount NOT BETWEEN 5 AND 10 ORDER BY id"
        )
        .await,
        [3]
    );
}

#[tokio::test]
async fn like_ilike_and_escape() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // names starting with 'a': apple, avocado.
    let names: Vec<String> = client
        .query(
            "SELECT name FROM pr WHERE name LIKE 'a%' ORDER BY name",
            &[],
        )
        .await
        .expect("like")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(names, ["apple", "avocado"]);
    // ILIKE folds case.
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM pr WHERE name ILIKE 'A%' ORDER BY id"
        )
        .await,
        [1, 5]
    );
    // `_` matches exactly one char: 'a_ple' won't match 'apple' (needs 'app_e').
    assert_eq!(
        ids(&client, "SELECT id FROM pr WHERE name LIKE 'app_e'").await,
        [1]
    );
    // `\` escapes a literal '%'.
    let lit: bool = client
        .query_one("SELECT 'a%b' LIKE 'a\\%b'", &[])
        .await
        .expect("escape")
        .get(0);
    assert!(lit);
}

#[tokio::test]
async fn searched_and_simple_case() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // searched CASE bucketing.
    let buckets: Vec<(i32, String)> = client
        .query(
            "SELECT id, CASE WHEN amount IS NULL THEN 'none' \
             WHEN amount >= 30 THEN 'big' ELSE 'small' END FROM pr ORDER BY id",
            &[],
        )
        .await
        .expect("case")
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(
        buckets,
        vec![
            (1, "small".into()),
            (2, "none".into()),
            (3, "big".into()),
            (4, "small".into()),
            (5, "small".into()),
        ]
    );
    // simple CASE; unmatched (no ELSE) -> NULL.
    let labels: Vec<Option<String>> = client
        .query(
            "SELECT CASE amount WHEN 10 THEN 'ten' WHEN 30 THEN 'thirty' END \
             FROM pr ORDER BY id",
            &[],
        )
        .await
        .expect("simple case")
        .iter()
        .map(|r| r.get::<_, Option<String>>(0))
        .collect();
    assert_eq!(
        labels,
        vec![
            Some("ten".into()),
            None,
            Some("thirty".into()),
            None,
            Some("ten".into()),
        ]
    );
}

#[tokio::test]
async fn select_distinct_dedups() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // amounts: 10, NULL, 30, 5, 10 -> distinct {5, 10, 30, NULL}.
    let amounts: Vec<Option<i32>> = client
        .query("SELECT DISTINCT amount FROM pr ORDER BY amount", &[])
        .await
        .expect("distinct")
        .iter()
        .map(|r| r.get::<_, Option<i32>>(0))
        .collect();
    // ORDER BY amount ASC puts NULLs last.
    assert_eq!(amounts, vec![Some(5), Some(10), Some(30), None]);
}

#[tokio::test]
async fn limit_offset_paging() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        ids(&client, "SELECT id FROM pr ORDER BY id LIMIT 2 OFFSET 2").await,
        [3, 4]
    );
    // OFFSET past the end yields no rows.
    assert_eq!(
        ids(&client, "SELECT id FROM pr ORDER BY id OFFSET 10").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn like_on_non_text_is_42804_and_session_survives() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .query("SELECT id FROM pr WHERE amount LIKE 'x'", &[])
        .await
        .expect_err("non-text LIKE");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "42804");
    // the session is still usable after the error.
    assert_eq!(ids(&client, "SELECT id FROM pr WHERE id = 1").await, [1]);
}

#[tokio::test]
async fn distinct_order_by_not_in_select_list_is_rejected() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .query("SELECT DISTINCT amount FROM pr ORDER BY id", &[])
        .await
        .expect_err("order by not in select list");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "42P10");
    assert_eq!(
        db.message(),
        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}
