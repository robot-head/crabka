//! SP34: uncorrelated subquery expressions, end-to-end over the wire.
//!
//! Covers scalar `(SELECT …)`, `x [NOT] IN (SELECT …)`, `[NOT] EXISTS (…)`, and
//! `x op ANY|SOME|ALL (…)`, plus the 21000 / 42601 error surface. The simple
//! query protocol exercises the engine's own execution and text encoding.

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

/// A subquery in FROM may omit its alias, as it has since `PostgreSQL` 16.
///
/// Several unnamed subqueries can appear in one FROM without collision, and the
/// columns they expose are usable unqualified.
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

/// A boolean connective settles on its left operand whenever the three-valued
/// table has a wildcard row for it — `false AND anything`, `true OR anything` —
/// and the right operand's correlated subquery must then not run at all.
///
/// The skip is observable rather than merely faster: the probe predicates below
/// divide by zero inside the right operand, on exactly the rows whose left
/// operand settles the answer. They return the same rows as their plain
/// counterparts only if that division never happens. The plain predicates
/// meanwhile pin the whole truth table, including the NULL row, where the left
/// operand settles nothing and the right operand must still run.
#[tokio::test]
async fn a_boolean_connective_skips_the_operand_it_cannot_need() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    client
        .simple_query(
            "CREATE TABLE flags (id int4, flag bool, k int4, da int4, dor int4); \
             INSERT INTO flags VALUES (1, true, 1, 1, 0), (2, false, 1, 0, 1), \
                                      (3, null, 1, 1, 1), (4, true, 0, 1, 0), \
                                      (5, false, 0, 0, 1), (6, null, 0, 1, 1)",
        )
        .await
        .expect("seed flags");

    // `EXISTS (SELECT 1 WHERE o.k = 1)` is correlated, so it is resolved per
    // outer row rather than once for the statement, and is true exactly on the
    // rows with k = 1. `da` / `dor` are zero exactly on the rows whose left
    // operand settles the AND / the OR.
    let exists = "EXISTS (SELECT 1 WHERE o.k = 1)";
    let exists_and_probe = "EXISTS (SELECT 1 WHERE 1 / o.da = 1 AND o.k = 1)";
    let exists_or_probe = "EXISTS (SELECT 1 WHERE 1 / o.dor = 1 AND o.k = 1)";

    for right in [exists, exists_and_probe] {
        for (wrapper, expected) in [
            ("{}", vec!["1"]),
            ("NOT ({})", vec!["2", "4", "5", "6"]),
            ("({}) IS NULL", vec!["3"]),
        ] {
            let predicate = wrapper.replace("{}", &format!("o.flag AND {right}"));
            let sql = format!("SELECT o.id FROM flags o WHERE {predicate} ORDER BY o.id");
            let expected = expected
                .into_iter()
                .map(|id| Some(id.to_string()))
                .collect::<Vec<_>>();
            assert!(col0(&client, &sql).await == expected, "{sql}");
        }
    }

    for right in [exists, exists_or_probe] {
        for (wrapper, expected) in [
            ("{}", vec!["1", "2", "3", "4"]),
            ("NOT ({})", vec!["5"]),
            ("({}) IS NULL", vec!["6"]),
        ] {
            let predicate = wrapper.replace("{}", &format!("o.flag OR {right}"));
            let sql = format!("SELECT o.id FROM flags o WHERE {predicate} ORDER BY o.id");
            let expected = expected
                .into_iter()
                .map(|id| Some(id.to_string()))
                .collect::<Vec<_>>();
            assert!(col0(&client, &sql).await == expected, "{sql}");
        }
    }

    // An operand that is reached still raises its error, so the probes above
    // are checking that the subquery was skipped and not that the division was
    // silently swallowed.
    assert!(
        err_code(
            &client,
            "SELECT o.id FROM flags o WHERE o.flag AND EXISTS (SELECT 1 WHERE 1 / o.dor = 1)"
        )
        .await
            == "22012"
    );
    assert!(
        err_code(
            &client,
            "SELECT o.id FROM flags o WHERE o.flag OR EXISTS (SELECT 1 WHERE 1 / o.da = 1)"
        )
        .await
            == "22012"
    );
}

/// Every column of every row of a simple query's result, as text.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(
                (0..row.len())
                    .map(|i| row.get(i).map(std::string::ToString::to_string))
                    .collect(),
            );
        }
    }
    out
}

/// The three-relation fixture the correlated select-list cases run over:
/// `x` has a row with no match (2), one with exactly one (1 and 3), and `z`
/// gives `1` two matching rows so the cardinality error has a source.
async fn seed_correlated(client: &tokio_postgres::Client) {
    for sql in [
        "CREATE TABLE u1 (a int4)",
        "CREATE TABLE u2 (b int4)",
        "CREATE TABLE u3 (c int4, d int4)",
        "INSERT INTO u1 VALUES (1), (2), (3)",
        "INSERT INTO u2 VALUES (1), (3)",
        "INSERT INTO u3 VALUES (1, 10), (1, 11), (3, 30)",
    ] {
        client.simple_query(sql).await.expect(sql);
    }
}

/// A correlated subquery in a select item is evaluated once per source row, in
/// every subquery form, and answers what `PostgreSQL` 18.4 answers.
#[tokio::test]
async fn correlated_subquery_forms_in_a_select_item() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    seed_correlated(&client).await;

    let cases: &[(&str, &[&[Option<&str>]])] = &[
        // A subquery matching no row is NULL, not a missing output row.
        (
            "SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("1")],
                &[Some("2"), None],
                &[Some("3"), Some("3")],
            ],
        ),
        (
            "SELECT x.a, EXISTS (SELECT 1 FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("t")],
                &[Some("2"), Some("f")],
                &[Some("3"), Some("t")],
            ],
        ),
        (
            "SELECT x.a, NOT EXISTS (SELECT 1 FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("f")],
                &[Some("2"), Some("t")],
                &[Some("3"), Some("f")],
            ],
        ),
        (
            "SELECT x.a, x.a IN (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("t")],
                &[Some("2"), Some("f")],
                &[Some("3"), Some("t")],
            ],
        ),
        (
            "SELECT x.a, x.a NOT IN (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("f")],
                &[Some("2"), Some("t")],
                &[Some("3"), Some("f")],
            ],
        ),
        (
            "SELECT x.a, x.a = ANY (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("t")],
                &[Some("2"), Some("f")],
                &[Some("3"), Some("t")],
            ],
        ),
        (
            "SELECT x.a, x.a >= ALL (SELECT y.b FROM u2 y WHERE y.b <= x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("t")],
                &[Some("2"), Some("t")],
                &[Some("3"), Some("t")],
            ],
        ),
        // `ARRAY(…)` folds the correlated rows into one array per source row.
        (
            "SELECT x.a, ARRAY(SELECT z.d FROM u3 z WHERE z.c = x.a ORDER BY z.d) \
             FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("{10,11}")],
                &[Some("2"), Some("{}")],
                &[Some("3"), Some("{30}")],
            ],
        ),
        // An aggregate inside the correlated subquery folds that row's matches.
        (
            "SELECT x.a, (SELECT max(z.d) FROM u3 z WHERE z.c = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("11")],
                &[Some("2"), None],
                &[Some("3"), Some("30")],
            ],
        ),
        // Correlation two levels deep: the innermost subquery reads the
        // outermost query's row, through a query block that is itself deferred.
        (
            "SELECT x.a, (SELECT (SELECT z.d FROM u3 z WHERE z.c = x.a AND z.d = 30) \
             FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), None],
                &[Some("2"), None],
                &[Some("3"), Some("30")],
            ],
        ),
        // Two correlated items in one select list, the second over a
        // two-relation FROM — the shape `psql`'s `\d` column query uses.
        (
            "SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a), \
             (SELECT z.d FROM u3 z, u2 y WHERE z.c = x.a AND y.b = x.a AND z.d = 30) \
             FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("1"), None],
                &[Some("2"), None, None],
                &[Some("3"), Some("3"), Some("30")],
            ],
        ),
        // The outer FROM may be a join; both of its relations are correlatable.
        (
            "SELECT x.a, w.b, (SELECT max(z.d) FROM u3 z WHERE z.c = x.a AND z.c = w.b) \
             FROM u1 x JOIN u2 w ON w.b = x.a ORDER BY x.a",
            &[
                &[Some("1"), Some("1"), Some("11")],
                &[Some("3"), Some("3"), Some("30")],
            ],
        ),
        // A correlated select item survives being read through a derived table.
        (
            "SELECT * FROM (SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a) AS c \
             FROM u1 x) s ORDER BY s.a",
            &[
                &[Some("1"), Some("1")],
                &[Some("2"), None],
                &[Some("3"), Some("3")],
            ],
        ),
        // A correlated select item alongside a correlated WHERE: the two use
        // separate deferred plans and must not disturb each other.
        (
            "SELECT x.a, (SELECT max(z.d) FROM u3 z WHERE z.c = x.a) FROM u1 x \
             WHERE EXISTS (SELECT 1 FROM u2 y WHERE y.b = x.a) ORDER BY x.a",
            &[&[Some("1"), Some("11")], &[Some("3"), Some("30")]],
        ),
        // An uncorrelated subquery in the same select list still folds once.
        (
            "SELECT x.a, (SELECT max(b) FROM u2) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("3")],
                &[Some("2"), Some("3")],
                &[Some("3"), Some("3")],
            ],
        ),
        // `SELECT *` must not expand to the hidden column the value is parked in.
        (
            "SELECT *, (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a",
            &[
                &[Some("1"), Some("1")],
                &[Some("2"), None],
                &[Some("3"), Some("3")],
            ],
        ),
    ];

    for (sql, expected) in cases {
        let expected: Vec<Vec<Option<String>>> = expected
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell.map(std::string::ToString::to_string))
                    .collect()
            })
            .collect();
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

/// A deferred select item still runs its subqueries lazily: a dead CASE branch
/// and an unreached COALESCE argument never execute theirs.
#[tokio::test]
async fn correlated_select_item_folds_lazily() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    seed_correlated(&client).await;

    // The division by zero sits in the branch taken only for a = 1, and the
    // branch that IS taken there is the constant one.
    assert!(
        rows(
            &client,
            "SELECT x.a, CASE WHEN x.a > 1 THEN (SELECT y.b FROM u2 y WHERE y.b = x.a) \
             ELSE -1 END FROM u1 x ORDER BY x.a"
        )
        .await
            == vec![
                vec![Some("1".into()), Some("-1".into())],
                vec![Some("2".into()), None],
                vec![Some("3".into()), Some("3".into())],
            ]
    );
    assert!(
        rows(
            &client,
            "SELECT x.a, COALESCE((SELECT y.b FROM u2 y WHERE y.b = x.a), 0) + 100 \
             FROM u1 x ORDER BY x.a"
        )
        .await
            == vec![
                vec![Some("1".into()), Some("101".into())],
                vec![Some("2".into()), Some("100".into())],
                vec![Some("3".into()), Some("103".into())],
            ]
    );
    // A reached branch still raises, so the probes above prove the dead branch
    // was skipped rather than that the error was swallowed.
    assert!(
        err_code(
            &client,
            "SELECT CASE WHEN x.a > 0 THEN (SELECT 1 / (x.a - 1)) ELSE 0 END FROM u1 x"
        )
        .await
            == "22012"
    );
}

/// `ORDER BY`, `DISTINCT` and `DISTINCT ON` reach a correlated select item
/// through every reference form `PostgreSQL` accepts.
#[tokio::test]
async fn correlated_select_item_orders_and_dedups() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    seed_correlated(&client).await;

    let cases: &[(&str, &[&[Option<&str>]])] = &[
        // By output ordinal.
        (
            "SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY 2 NULLS FIRST",
            &[
                &[Some("2"), None],
                &[Some("1"), Some("1")],
                &[Some("3"), Some("3")],
            ],
        ),
        // By output label.
        (
            "SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a) AS c \
             FROM u1 x ORDER BY c DESC NULLS LAST",
            &[
                &[Some("3"), Some("3")],
                &[Some("1"), Some("1")],
                &[Some("2"), None],
            ],
        ),
        // By the correlated expression itself, which is not in the select list.
        (
            "SELECT x.a FROM u1 x ORDER BY (SELECT y.b FROM u2 y WHERE y.b = x.a) NULLS FIRST",
            &[&[Some("2")], &[Some("1")], &[Some("3")]],
        ),
        // DISTINCT dedups the projected values, NULL included.
        (
            "SELECT DISTINCT (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x \
             ORDER BY 1 NULLS FIRST",
            &[&[None], &[Some("1")], &[Some("3")]],
        ),
        // DISTINCT ON recognizes that its key and the ORDER BY key are the same
        // correlated expression.
        (
            "SELECT DISTINCT ON ((SELECT y.b FROM u2 y WHERE y.b = x.a)) x.a FROM u1 x \
             ORDER BY (SELECT y.b FROM u2 y WHERE y.b = x.a) NULLS FIRST, x.a",
            &[&[Some("2")], &[Some("1")], &[Some("3")]],
        ),
        // LIMIT applies to the ordered output, as it does without correlation.
        (
            "SELECT x.a, (SELECT y.b FROM u2 y WHERE y.b = x.a) FROM u1 x ORDER BY x.a LIMIT 2",
            &[&[Some("1"), Some("1")], &[Some("2"), None]],
        ),
    ];

    for (sql, expected) in cases {
        let expected: Vec<Vec<Option<String>>> = expected
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell.map(std::string::ToString::to_string))
                    .collect()
            })
            .collect();
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

/// The errors a correlated select item raises are the ones `PostgreSQL` raises.
#[tokio::test]
async fn correlated_select_item_error_surface() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    seed_correlated(&client).await;

    let cases = [
        // More than one row from a scalar subquery.
        (
            "SELECT x.a, (SELECT z.d FROM u3 z WHERE z.c = x.a) FROM u1 x",
            "21000",
        ),
        // More than one column from a scalar subquery.
        (
            "SELECT x.a, (SELECT z.c, z.d FROM u3 z WHERE z.c = x.a) FROM u1 x",
            "42601",
        ),
        // A qualifier that is in no FROM clause at any level is still 42P01,
        // rather than being treated as a correlation that happens to fail.
        (
            "SELECT x.a, (SELECT 1 FROM u2 y WHERE y.b = nosuch.a) FROM u1 x",
            "42P01",
        ),
    ];

    for (sql, code) in cases {
        assert!(err_code(&client, sql).await == code, "{sql}");
    }
}
