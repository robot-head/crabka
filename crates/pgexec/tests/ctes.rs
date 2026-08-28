use std::{error::Error, sync::Arc, time::Duration};

use bytes::BytesMut;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{
    NoTls,
    types::{Format, IsNull, ToSql, Type, to_sql_checked},
};

#[derive(Debug)]
struct TextInt4(&'static str);

impl ToSql for TextInt4 {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::INT4 {
            return Err("TextInt4 only supports int4".into());
        }
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT4
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

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

async fn connect_new() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(spawn().await)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            );
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

async fn prepare_err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .prepare(sql)
        .await
        .expect_err("expected prepare error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn a_data_modifying_cte_describes_its_returning_columns() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE cte_returning (id int4)")
        .await
        .expect("create table");

    let statement = c
        .prepare(
            "WITH inserted AS (INSERT INTO cte_returning VALUES (1) RETURNING id) \
             SELECT id FROM inserted",
        )
        .await
        .expect("describe data-modifying CTE");
    assert_eq!(statement.columns().len(), 1);
    assert_eq!(statement.columns()[0].name(), "id");
    assert_eq!(statement.columns()[0].type_(), &Type::INT4);
}

#[tokio::test]
async fn simple_cte_later_cte_and_forward_reference() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        err_code(
            &c,
            "WITH b AS (SELECT * FROM a), a AS (SELECT 1 AS x) SELECT * FROM b"
        )
        .await,
        "42P01"
    );
}

#[tokio::test]
async fn cte_shadows_base_table_and_can_be_reused() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE src (x int4)")
        .await
        .expect("create src");
    c.simple_query("INSERT INTO src VALUES (9)")
        .await
        .expect("insert src");
    assert_eq!(
        rows(
            &c,
            "WITH src AS (SELECT 1 AS x) SELECT a.x, b.x FROM src a, src b"
        )
        .await,
        vec![vec![Some("1".into()), Some("1".into())]]
    );
}

#[tokio::test]
async fn cte_column_alias_lists_follow_postgres() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "WITH c(y) AS (SELECT 7 AS x) SELECT y FROM c").await,
        vec![vec![Some("7".into())]]
    );
    // More aliases than the query has columns is 42P10; FEWER is legal, and the
    // unnamed trailing columns keep the names the query gave them.
    assert_eq!(
        err_code(&c, "WITH c(y, z) AS (SELECT 7 AS x) SELECT * FROM c").await,
        "42P10"
    );
    assert_eq!(
        rows(
            &c,
            "WITH c(y) AS (SELECT 7 AS x, 8 AS z) SELECT y, z FROM c"
        )
        .await,
        vec![vec![Some("7".into()), Some("8".into())]]
    );
    // A `WITH RECURSIVE` item that does not refer to itself is an ordinary CTE.
    assert_eq!(
        rows(&c, "WITH RECURSIVE r AS (SELECT 1 AS x) SELECT * FROM r").await,
        vec![vec![Some("1".into())]]
    );
}

#[tokio::test]
async fn values_and_set_operation_ctes_work() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH v(x) AS (VALUES (2), (1)) SELECT x FROM v ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH u(x) AS (SELECT 1 UNION SELECT 2) SELECT x FROM u ORDER BY x DESC"
        )
        .await,
        vec![vec![Some("2".into())], vec![Some("1".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), \
             u AS (SELECT x FROM a UNION SELECT 2) \
             SELECT x FROM u ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), \
             u AS (SELECT (SELECT x FROM a) AS x UNION SELECT 2) \
             SELECT x FROM u ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn nested_with_scopes_through_derived_tables_subqueries_and_describe() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT * FROM (WITH c AS (VALUES (2)) SELECT * FROM c) AS d(x)"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT EXISTS (WITH d AS (SELECT * FROM c) SELECT 1 FROM d)"
        )
        .await,
        vec![vec![Some("t".into())]]
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) SELECT x FROM c")
        .await
        .expect("describe CTE select");
    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["x"]);

    let stmt = c
        .prepare("WITH u(x) AS (SELECT 1 UNION SELECT 2) SELECT x FROM u")
        .await
        .expect("describe set-op CTE select");
    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["x"]);

    let stmt = c
        .prepare(
            "WITH c(x) AS (VALUES (1)) SELECT * FROM (WITH c(y) AS (VALUES (2)) SELECT y FROM c) AS d",
        )
        .await
        .expect("describe nested CTE shadowing");
    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["y"]);

    let stmt = c
        .prepare("SELECT (WITH c AS (SELECT 1 AS x) SELECT x FROM c)")
        .await
        .expect("describe scalar subquery CTE");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) SELECT (WITH c(y) AS (VALUES (2)) SELECT y FROM c)")
        .await
        .expect("describe scalar subquery CTE shadowing");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c))")
        .await
        .expect("describe VALUES scalar subquery CTE");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    assert_eq!(
        rows(&c, "WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c))").await,
        vec![vec![Some("1".into())]]
    );

    assert_eq!(
        rows(
            &c,
            "WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c)) UNION SELECT 2 ORDER BY 1"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn describe_derived_cte_captures_outer_cte_scope() {
    let c = connect_new().await;
    let stmt = c
        .prepare(
            "WITH c(x) AS (VALUES (1)) \
             SELECT * FROM (WITH d AS (SELECT x FROM c) SELECT x FROM d) AS nested",
        )
        .await
        .expect("describe derived CTE that captures an outer CTE");
    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["x"]);
}

#[tokio::test]
async fn extended_bind_in_derived_cte_captures_outer_cte_scope() {
    let c = connect_new().await;
    let stmt = c
        .prepare(
            "WITH c(x) AS (VALUES (1)) \
              SELECT x FROM (WITH d AS (SELECT x FROM c WHERE x = $1) SELECT x FROM d) nested",
        )
        .await
        .expect("prepare derived CTE parameter that captures an outer CTE");
    assert_eq!(stmt.params(), &[tokio_postgres::types::Type::INT4]);

    let result = c
        .query(&stmt, &[&TextInt4("1")])
        .await
        .expect("bind and execute derived CTE parameter");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get::<_, i32>(0), 1);
}

#[tokio::test]
async fn locking_select_rejects_ctes() {
    let c = connect_new().await;
    assert_eq!(
        err_code(&c, "WITH c AS (SELECT 1 AS x) SELECT * FROM c FOR UPDATE").await,
        "0A000"
    );
    assert_eq!(
        err_code(
            &c,
            "WITH RECURSIVE c AS (SELECT 1 AS x) SELECT * FROM c FOR UPDATE"
        )
        .await,
        "0A000"
    );
}

#[tokio::test]
async fn nested_locking_inside_cte_body_is_rejected() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (x int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert t");
    assert_eq!(
        err_code(
            &c,
            "WITH c AS (SELECT * FROM (SELECT x FROM t FOR UPDATE) d) SELECT * FROM c"
        )
        .await,
        "0A000"
    );
}

#[tokio::test]
async fn locking_inside_cte_body_is_rejected() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (x int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert t");
    assert_eq!(
        err_code(&c, "WITH c AS (SELECT x FROM t FOR UPDATE) SELECT * FROM c").await,
        "0A000"
    );
}

#[tokio::test]
async fn describe_rejects_nested_locking_selects() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (x int4)")
        .await
        .expect("create t");

    c.prepare("SELECT x FROM t FOR UPDATE")
        .await
        .expect("top-level locking describe");

    assert_eq!(
        prepare_err_code(&c, "WITH c AS (SELECT 1 AS x) SELECT * FROM c FOR UPDATE").await,
        "0A000"
    );
    assert_eq!(
        prepare_err_code(&c, "WITH c AS (SELECT x FROM t FOR UPDATE) SELECT * FROM c").await,
        "0A000"
    );
    assert_eq!(
        prepare_err_code(&c, "SELECT * FROM (SELECT x FROM t FOR UPDATE) d").await,
        "0A000"
    );
}

#[tokio::test]
async fn create_recursive_view_runs_as_an_implicit_recursive_cte() {
    let c = connect_new().await;
    c.simple_query(
        "CREATE RECURSIVE VIEW nums(n) AS \
         VALUES (1) UNION ALL SELECT n + 1 FROM nums WHERE n < 3",
    )
    .await
    .expect("create recursive view");

    assert_eq!(
        rows(&c, "SELECT n FROM nums ORDER BY n").await,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
        ]
    );
    let breadth = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
                   SEARCH BREADTH FIRST BY n SET seq SELECT n, seq FROM t";
    let breadth_statement = c.prepare(breadth).await.expect("describe breadth search");
    assert_eq!(breadth_statement.columns()[1].type_(), &Type::RECORD);

    let marked = "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL \
                  SELECT CASE WHEN n < 2 THEN n + 1 ELSE 1 END FROM t) \
                  SEARCH DEPTH FIRST BY n SET seq \
                  CYCLE n SET mark TO 'yes' DEFAULT 'no' USING path \
                  SELECT n, seq, mark, path FROM t ORDER BY path";
    assert_eq!(
        rows(&c, marked).await,
        vec![
            vec![
                Some("1".into()),
                Some("{(1)}".into()),
                Some("no".into()),
                Some("{(1)}".into()),
            ],
            vec![
                Some("2".into()),
                Some("{(1),(2)}".into()),
                Some("no".into()),
                Some("{(1),(2)}".into()),
            ],
            vec![
                Some("1".into()),
                Some("{(1),(2),(1)}".into()),
                Some("yes".into()),
                Some("{(1),(2),(1)}".into()),
            ],
        ]
    );
    let statement = c.prepare(marked).await.expect("describe recursive extras");
    assert_eq!(statement.columns()[2].type_(), &Type::TEXT);

    let cycle_statement = c
        .prepare(
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 2) \
             CYCLE n SET mark USING path SELECT n, mark, path FROM t",
        )
        .await
        .expect("describe cycle extras");
    assert_eq!(cycle_statement.columns()[1].type_(), &Type::BOOL);
    assert_eq!(cycle_statement.columns()[2].type_(), &Type::RECORD_ARRAY);
}

#[tokio::test]
async fn recursive_cte_search_and_cycle_columns_follow_their_parent_rows() {
    let c = connect_new().await;

    assert_eq!(
        rows(
            &c,
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
             SEARCH BREADTH FIRST BY n SET seq \
             SELECT n, seq FROM t ORDER BY seq",
        )
        .await,
        vec![
            vec![Some("1".into()), Some("(0,1)".into())],
            vec![Some("2".into()), Some("(1,2)".into())],
            vec![Some("3".into()), Some("(2,3)".into())],
        ]
    );

    assert_eq!(
        rows(
            &c,
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
             SEARCH DEPTH FIRST BY n SET seq \
             SELECT n, seq FROM t ORDER BY seq",
        )
        .await,
        vec![
            vec![Some("1".into()), Some("{(1)}".into())],
            vec![Some("2".into()), Some("{(1),(2)}".into())],
            vec![Some("3".into()), Some("{(1),(2),(3)}".into())],
        ]
    );

    assert_eq!(
        rows(
            &c,
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL \
             SELECT CASE WHEN n < 3 THEN n + 1 ELSE 1 END FROM t) \
             CYCLE n SET is_cycle USING path \
             SELECT n, is_cycle, path FROM t ORDER BY path",
        )
        .await,
        vec![
            vec![Some("1".into()), Some("f".into()), Some("{(1)}".into())],
            vec![Some("2".into()), Some("f".into()), Some("{(1),(2)}".into())],
            vec![
                Some("3".into()),
                Some("f".into()),
                Some("{(1),(2),(3)}".into())
            ],
            vec![
                Some("1".into()),
                Some("t".into()),
                Some("{(1),(2),(3),(1)}".into()),
            ],
        ]
    );
}

#[tokio::test]
async fn recursive_cte_stops_when_a_plain_outer_limit_has_enough_rows() {
    let c = connect_new().await;
    let rows = tokio::time::timeout(
        Duration::from_secs(1),
        rows(
            &c,
            "WITH RECURSIVE test AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM test) \
             SEARCH DEPTH FIRST BY x SET y SELECT * FROM test LIMIT 5",
        ),
    )
    .await
    .expect("outer LIMIT must stop unbounded recursive CTE production");
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("{(1)}".into())],
            vec![Some("2".into()), Some("{(1),(2)}".into())],
            vec![Some("3".into()), Some("{(1),(2),(3)}".into())],
            vec![Some("4".into()), Some("{(1),(2),(3),(4)}".into())],
            vec![Some("5".into()), Some("{(1),(2),(3),(4),(5)}".into())],
        ]
    );
}
