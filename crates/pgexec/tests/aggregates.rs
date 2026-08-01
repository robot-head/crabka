//! SP27: aggregate functions + GROUP BY / HAVING, end-to-end over the wire
//! (simple + extended protocol), including result types and error SQLSTATEs.

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

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE sales (region text, amount int4)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO sales VALUES \
             ('west', 10), ('west', 20), ('east', 5), ('east', 5), ('north', 100)",
        )
        .await
        .expect("insert");
}

#[tokio::test]
async fn group_by_with_count_and_sum() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // count(*) and sum(amount) are bigint; group order pinned by ORDER BY region.
    let rows = client
        .query(
            "SELECT region, count(*), sum(amount) FROM sales GROUP BY region ORDER BY region",
            &[],
        )
        .await
        .expect("group by");
    let got: Vec<(String, i64, i64)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("east".to_string(), 2, 10),
            ("north".to_string(), 1, 100),
            ("west".to_string(), 2, 30),
        ]
    );
}

#[tokio::test]
async fn having_filters_groups_and_orders_by_aggregate() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // Only regions whose total exceeds 10, ordered by that total descending.
    let rows = client
        .query(
            "SELECT region, sum(amount) FROM sales GROUP BY region \
             HAVING sum(amount) > 10 ORDER BY sum(amount) DESC",
            &[],
        )
        .await
        .expect("having");
    let got: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        got,
        vec![("north".to_string(), 100), ("west".to_string(), 30)]
    );
}

#[tokio::test]
async fn count_distinct_and_min_max() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let row = client
        .query_one(
            "SELECT count(DISTINCT amount), min(amount), max(region) FROM sales",
            &[],
        )
        .await
        .expect("distinct/min/max");
    let distinct_amounts: i64 = row.get(0); // {10,20,5,100} -> 4
    let min_amount: i32 = row.get(1); // 5  (min preserves int4)
    let max_region: String = row.get(2); // 'west'
    assert_eq!(
        (distinct_amounts, min_amount, max_region.as_str()),
        (4, 5, "west")
    );
}

#[tokio::test]
async fn overloaded_plpgsql_wrapper_resolves_aggregate_argument_from_input_scope() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE zz_input (i int4); \
             INSERT INTO zz_input VALUES (1), (3), (2); \
             CREATE FUNCTION zz(value int4) RETURNS int4 LANGUAGE plpgsql AS \
             $$ BEGIN RETURN value + 1; END $$; \
             CREATE FUNCTION zz(value text) RETURNS text LANGUAGE plpgsql AS \
             $$ BEGIN RETURN value || '!'; END $$",
        )
        .await
        .expect("setup overloaded wrapper");

    let messages = client
        .simple_query("SELECT zz(max(i)) FROM zz_input")
        .await
        .expect("resolve zz(integer)");
    let value = messages.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(value, Some("4"));
}

#[tokio::test]
async fn bare_aggregate_over_empty_table_is_one_row() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE empty_t (v int4)")
        .await
        .expect("create");
    let row = client
        .query_one("SELECT count(*), sum(v) FROM empty_t", &[])
        .await
        .expect("bare agg");
    let n: i64 = row.get(0);
    let s: Option<i64> = row.get(1); // sum of no rows -> NULL
    assert_eq!(n, 0);
    assert_eq!(s, None);
}

#[tokio::test]
async fn ungrouped_column_errors_42803_and_session_survives() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .batch_execute("SELECT region, amount FROM sales GROUP BY region")
        .await
        .expect_err("ungrouped amount");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42803");
    // Session still usable.
    let row = client
        .query_one("SELECT count(*) FROM sales", &[])
        .await
        .expect("recovered");
    let n: i64 = row.get(0);
    assert_eq!(n, 5);
}

#[tokio::test]
async fn unknown_function_errors_42883() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .batch_execute("SELECT frobnicate(amount), count(*) FROM sales GROUP BY amount")
        .await
        .expect_err("unknown fn");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42883");
}

#[tokio::test]
async fn for_update_with_group_by_errors_0a000() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .batch_execute("SELECT region FROM sales GROUP BY region FOR UPDATE")
        .await
        .expect_err("for update + group by");
    assert_eq!(err.as_db_error().expect("db").code().code(), "0A000");
}
