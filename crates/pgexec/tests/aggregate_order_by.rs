//! `agg(args ORDER BY key [, …])` — the order an aggregate folds its own input
//! in, end-to-end over the wire.
//!
//! Every expected value in this file was taken from `PostgreSQL` 18.4 running the
//! same statements over the same rows under `LC_ALL=C`, which is why `'B'` sorts
//! before `'a'` throughout.

use std::sync::Arc;

use assert2::assert;
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

/// `t(a int, b text, c float8)` plus `g(grp int, a int, b text)`, the two shapes
/// every case below reads.
async fn seeded() -> tokio_postgres::Client {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (a int, b text, c float8);
             INSERT INTO t VALUES (3,'c',3.5),(1,'a',1.5),(2,'b',2.5),(2,'B',NULL),(NULL,NULL,0.5);
             CREATE TABLE g (grp int, a int, b text);
             INSERT INTO g VALUES (1,3,'x'),(1,1,'z'),(1,2,'y'),(2,9,'p'),(2,7,'q'),(3,NULL,NULL)",
        )
        .await
        .expect("seed");
    client
}

/// The one-row, one-column answer to `sql`, rendered as text so a case table can
/// hold arrays, strings and numbers side by side. `None` is SQL NULL.
async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    let statement = format!("SELECT ({sql})::text");
    let rows = client.query(&statement, &[]).await.expect(sql);
    assert!(rows.len() == 1, "{sql}");
    rows[0].get(0)
}

async fn error_of(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = client
        .query(sql, &[])
        .await
        .expect_err(sql)
        .as_db_error()
        .expect("a database error")
        .clone();
    (error.code().code().to_string(), error.message().to_string())
}

#[tokio::test]
async fn a_sort_orders_the_rows_each_aggregate_folds() {
    let client = seeded().await;
    let cases = [
        // direction and the NULL placement each direction defaults to
        ("array_agg(a ORDER BY a) FROM t", "{1,2,2,3,NULL}"),
        ("array_agg(a ORDER BY a DESC) FROM t", "{NULL,3,2,2,1}"),
        ("array_agg(b ORDER BY b) FROM t", "{B,a,b,c,NULL}"),
        (
            "array_agg(b ORDER BY b NULLS FIRST) FROM t",
            "{NULL,B,a,b,c}",
        ),
        (
            "array_agg(b ORDER BY b DESC NULLS LAST) FROM t",
            "{c,b,a,B,NULL}",
        ),
        (
            "array_agg(a ORDER BY a ASC NULLS FIRST) FROM t",
            "{NULL,1,2,2,3}",
        ),
        // several keys, mixed directions
        ("array_agg(a ORDER BY b, a) FROM t", "{2,1,2,3,NULL}"),
        (
            "array_agg(a ORDER BY b DESC, a ASC) FROM t",
            "{NULL,3,2,1,2}",
        ),
        (
            "array_agg(a ORDER BY c NULLS FIRST, b DESC) FROM t",
            "{2,NULL,1,2,3}",
        ),
        // a key that is not an argument, including a computed one
        ("array_agg(a ORDER BY b) FROM t", "{2,1,2,3,NULL}"),
        ("array_agg(a ORDER BY -a) FROM t", "{3,2,2,1,NULL}"),
        ("array_agg(a ORDER BY a + 10 DESC) FROM t", "{NULL,3,2,2,1}"),
        // every non-NULL length ties, so the stable sort keeps arrival order
        ("string_agg(b, ',' ORDER BY length(b)) FROM t", "c,a,b,B"),
        // the collecting and joining aggregates
        ("string_agg(b, ',' ORDER BY b) FROM t", "B,a,b,c"),
        ("string_agg(b, ',' ORDER BY b DESC) FROM t", "c,b,a,B"),
        ("string_agg(b, '|' ORDER BY a DESC, b) FROM t", "c|B|b|a"),
        ("json_agg(a ORDER BY a) FROM t", "[1, 2, 2, 3, null]"),
        ("jsonb_agg(a ORDER BY a DESC) FROM t", "[null, 3, 2, 2, 1]"),
        // an order-independent aggregate still accepts the clause
        ("sum(a ORDER BY a) FROM t", "8"),
        ("sum(a ORDER BY b DESC) FROM t", "8"),
        ("count(a ORDER BY a DESC) FROM t", "4"),
        ("min(a ORDER BY b) FROM t", "1"),
        ("max(b ORDER BY a) FROM t", "c"),
        ("bool_and(a > 0 ORDER BY a) FROM t", "true"),
        // the sort applies to the rows FILTER kept, not the ones it dropped
        (
            "array_agg(a ORDER BY a DESC) FILTER (WHERE a > 1) FROM t",
            "{3,2,2}",
        ),
        (
            "string_agg(b, ',' ORDER BY b) FILTER (WHERE b <> 'a') FROM t",
            "B,b,c",
        ),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, &format!("SELECT {sql}")).await.as_deref() == Some(expected),
            "{sql}"
        );
    }
}

/// A sort key inside an aggregate is an expression, never an output-column
/// position and never an output label — `PostgreSQL`'s SQL99 rule for the clause.
#[tokio::test]
async fn a_sort_key_is_never_an_output_reference() {
    let client = seeded().await;
    // Every row's key is the constant one, so the stable sort keeps arrival
    // order rather than sorting by the first output column.
    for sql in [
        "SELECT array_agg(a ORDER BY 1) FROM t",
        "SELECT array_agg(a ORDER BY 1 DESC) FROM t",
        "SELECT array_agg(a ORDER BY 2) FROM t",
    ] {
        assert!(
            scalar(&client, sql).await.as_deref() == Some("{3,1,2,2,NULL}"),
            "{sql}"
        );
    }
    let (sqlstate, message) =
        error_of(&client, "SELECT array_agg(a ORDER BY zz) AS zz FROM t").await;
    assert!(sqlstate == "42703");
    assert!(message.contains("zz"), "{message}");
}

#[tokio::test]
async fn distinct_deduplicates_the_arguments_the_sort_ordered() {
    let client = seeded().await;
    let cases = [
        ("array_agg(DISTINCT a ORDER BY a) FROM t", "{1,2,3,NULL}"),
        (
            "array_agg(DISTINCT a ORDER BY a DESC) FROM t",
            "{NULL,3,2,1}",
        ),
        (
            "array_agg(DISTINCT a ORDER BY a DESC NULLS LAST) FROM t",
            "{3,2,1,NULL}",
        ),
        // the sort covers one of two arguments; the other still deduplicates
        ("string_agg(DISTINCT b, ',' ORDER BY b) FROM t", "B,a,b,c"),
        ("sum(DISTINCT a ORDER BY a) FROM t", "6"),
        // an integer literal carries its own type in both places, so it matches
        ("array_agg(DISTINCT 1 ORDER BY 1) FROM t", "{1}"),
        // FILTER runs before the deduplication, as it does without a sort
        (
            "array_agg(DISTINCT a ORDER BY a) FILTER (WHERE a IS NOT NULL) FROM t",
            "{1,2,3}",
        ),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, &format!("SELECT {sql}")).await.as_deref() == Some(expected),
            "{sql}"
        );
    }
}

/// Under `DISTINCT` the rows are deduplicated on the argument tuple, so a sort
/// key that is not one of the arguments would order rows that no longer exist.
#[tokio::test]
async fn distinct_refuses_a_sort_key_that_is_not_an_argument() {
    let client = seeded().await;
    for sql in [
        "SELECT array_agg(DISTINCT a ORDER BY b) FROM t",
        "SELECT array_agg(DISTINCT a ORDER BY a + 1) FROM t",
        "SELECT string_agg(DISTINCT b, ',' ORDER BY b || '') FROM t",
        // an untyped literal is `unknown` as a sort key and coerced as an
        // argument, so it never matches however identically it reads
        "SELECT string_agg(DISTINCT b, ',' ORDER BY ',') FROM t",
        "SELECT array_agg(DISTINCT a ORDER BY NULL) FROM t",
    ] {
        let (sqlstate, message) = error_of(&client, sql).await;
        assert!(sqlstate == "42P10", "{sql}");
        assert!(
            message
                == "in an aggregate with DISTINCT, ORDER BY expressions must appear \
                        in argument list",
            "{sql}"
        );
    }
    // Casting both sides to the same type is what makes the pair match again.
    assert!(
        scalar(
            &client,
            "SELECT string_agg(DISTINCT b, ','::text ORDER BY ','::text) FROM t"
        )
        .await
        .as_deref()
            == Some("B,a,b,c")
    );
}

#[tokio::test]
async fn the_sort_runs_per_group_and_survives_an_empty_one() {
    let client = seeded().await;
    let rows = client
        .query(
            "SELECT grp, array_agg(a ORDER BY a DESC)::text, \
             string_agg(b, ',' ORDER BY b DESC) \
             FROM g GROUP BY grp ORDER BY grp",
            &[],
        )
        .await
        .expect("grouped");
    let got: Vec<(i32, String, Option<String>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert!(
        got == vec![
            (1, "{3,2,1}".to_string(), Some("z,y,x".to_string())),
            (2, "{9,7}".to_string(), Some("q,p".to_string())),
            (3, "{NULL}".to_string(), None),
        ]
    );
    // HAVING sees the sorted aggregate like any other.
    let kept: Vec<i32> = client
        .query(
            "SELECT grp FROM g GROUP BY grp \
             HAVING array_agg(a ORDER BY a) IS NOT NULL ORDER BY grp",
            &[],
        )
        .await
        .expect("having")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(kept == vec![1, 2, 3]);
    // Zero rows is SQL NULL, not an empty array — the sort changes nothing.
    for sql in [
        "SELECT array_agg(a ORDER BY a) FROM t WHERE false",
        "SELECT sum(a ORDER BY a) FROM t WHERE false",
        "SELECT array_agg(a ORDER BY a) FILTER (WHERE false) FROM t",
    ] {
        assert!(scalar(&client, sql).await.is_none(), "{sql}");
    }
    assert!(
        scalar(&client, "SELECT count(a ORDER BY a) FROM t WHERE false")
            .await
            .as_deref()
            == Some("0")
    );
}

/// A scalar function sees one row at a time and has nothing to order.
#[tokio::test]
async fn a_sort_on_a_non_aggregate_is_refused() {
    let client = seeded().await;
    for (sql, name) in [
        ("SELECT abs(1 ORDER BY 1)", "abs"),
        ("SELECT length('x' ORDER BY 1)", "length"),
    ] {
        let (sqlstate, message) = error_of(&client, sql).await;
        assert!(sqlstate == "42809", "{sql}");
        assert!(
            message == format!("ORDER BY specified, but {name} is not an aggregate function"),
            "{sql}"
        );
    }
    // An aggregate inside the sort is the ordinary nesting refusal.
    let (_, message) = error_of(&client, "SELECT array_agg(a ORDER BY sum(a)) FROM t").await;
    assert!(
        message == "aggregate function calls cannot be nested",
        "{message}"
    );
}

/// The clause reaches the executor through every path that carries a call: a
/// subquery, a set operation, a grouping set, a view definition, and a routine
/// body.
#[tokio::test]
async fn the_sort_travels_with_the_call_through_every_rewrite() {
    let client = seeded().await;
    for (sql, expected) in [
        (
            "SELECT (SELECT array_agg(a ORDER BY a DESC) FROM t)",
            "{NULL,3,2,2,1}",
        ),
        (
            "SELECT array_agg(x ORDER BY x DESC) FROM (SELECT a AS x FROM t) s",
            "{NULL,3,2,2,1}",
        ),
    ] {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(expected),
            "{sql}"
        );
    }
    let united: Vec<Option<String>> = client
        .query(
            "SELECT array_agg(a ORDER BY a)::text FROM t \
             UNION ALL SELECT array_agg(a ORDER BY a DESC)::text FROM t",
            &[],
        )
        .await
        .expect("union")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(
        united
            == vec![
                Some("{1,2,2,3,NULL}".to_string()),
                Some("{NULL,3,2,2,1}".to_string()),
            ]
    );
    // A view stores the call and must read back with its sort, or the stored
    // definition computes a different array from the one the view was created
    // with.
    client
        .batch_execute(
            "CREATE VIEW v AS \
             SELECT grp, array_agg(DISTINCT a ORDER BY a DESC) AS ag FROM g GROUP BY grp",
        )
        .await
        .expect("create view");
    let definition: String = client
        .query_one("SELECT pg_get_viewdef('v'::regclass, true)", &[])
        .await
        .expect("viewdef")
        .get(0);
    assert!(
        definition.contains("array_agg(DISTINCT a ORDER BY a DESC) AS ag"),
        "{definition}"
    );
    let through_view: Vec<Option<String>> = client
        .query("SELECT ag::text FROM v ORDER BY grp", &[])
        .await
        .expect("select from view")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(
        through_view
            == vec![
                Some("{3,2,1}".to_string()),
                Some("{9,7}".to_string()),
                Some("{NULL}".to_string()),
            ]
    );
    // A plpgsql body carries the call through the variable substitution the
    // routine runtime performs — the shape `triggers` exercises.
    client
        .batch_execute(
            "CREATE FUNCTION joined(sep text) RETURNS text LANGUAGE plpgsql AS $$
               BEGIN
                 RETURN (SELECT string_agg(b, sep ORDER BY b DESC) FROM t);
               END $$",
        )
        .await
        .expect("create function");
    assert!(
        scalar(&client, "SELECT joined('/')").await.as_deref() == Some("c/b/a/B"),
        "plpgsql body"
    );
}

/// `JSON_ARRAYAGG(e ORDER BY k)` is the SQL-standard spelling of the same sort
/// and lowers onto the same aggregate.
#[tokio::test]
async fn the_sql_standard_json_aggregate_spelling_carries_its_sort() {
    let client = seeded().await;
    // The rows carrying a NULL are excluded because this engine's
    // `JSON_ARRAYAGG` does not yet apply the standard's ABSENT ON NULL default —
    // a separate gap from the sort, and the one thing these rows would measure.
    for (sql, expected) in [
        (
            "JSON_ARRAYAGG(a ORDER BY a) FROM t WHERE a IS NOT NULL",
            "[1, 2, 2, 3]",
        ),
        (
            "JSON_ARRAYAGG(a ORDER BY a DESC) FROM t WHERE a IS NOT NULL",
            "[3, 2, 2, 1]",
        ),
    ] {
        assert!(
            scalar(&client, &format!("SELECT {sql}")).await.as_deref() == Some(expected),
            "{sql}"
        );
    }
}

/// A sort key is a sub-expression like any other, so the generic expression walk
/// has to visit it. It did not, and a view whose only reference to a table sat
/// inside one was recorded as not depending on that table — `DROP TABLE` then
/// succeeded and left the view broken.
#[tokio::test]
async fn a_relation_named_only_inside_a_sort_key_is_a_view_dependency() {
    let client = seeded().await;
    client
        .batch_execute(
            "CREATE TABLE other (x int);
             INSERT INTO other VALUES (5),(7);
             CREATE VIEW wv AS \
               SELECT array_agg(a ORDER BY (SELECT max(x) FROM other), b) AS ag FROM t",
        )
        .await
        .expect("seed");
    assert!(
        scalar(&client, "SELECT ag FROM wv").await.as_deref() == Some("{2,1,2,3,NULL}"),
        "the sort key's subquery is evaluated"
    );
    let (sqlstate, message) = error_of(&client, "DROP TABLE other").await;
    assert!(sqlstate == "2BP01", "{message}");
    assert!(message.contains("other objects depend on it"), "{message}");
    // The view still reads after the refused drop.
    assert!(scalar(&client, "SELECT ag FROM wv").await.is_some());
}
