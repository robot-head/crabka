//! SP33: SQL joins, end-to-end over the wire with the extended protocol.
//!
//! The tests drive `tokio_postgres`. They cover every join type, the comma
//! form, USING/NATURAL, qualified refs, `a.*`, self joins and multi-way joins,
//! derived tables, aggregate-over-join, and the column-resolution error
//! surface (42702 / 42703 / 42P01).

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

/// Seed the standard emp/dept fixture.
///
/// emp 'cy' has a NULL `dept_id`, so it is unmatched on the left. dept 'ops'(30)
/// has no employee, so it is unmatched on the right.
async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE emp (id int4, name text, dept_id int4)")
        .await
        .expect("create emp");
    client
        .batch_execute("CREATE TABLE dept (id int4, dname text)")
        .await
        .expect("create dept");
    client
        .batch_execute("INSERT INTO emp VALUES (1,'ann',10),(2,'bob',20),(3,'cy',NULL)")
        .await
        .expect("insert emp");
    client
        .batch_execute("INSERT INTO dept VALUES (10,'eng'),(20,'sales'),(30,'ops')")
        .await
        .expect("insert dept");
}

fn err_code(e: &tokio_postgres::Error) -> String {
    e.as_db_error().expect("db error").code().code().to_string()
}

#[tokio::test]
async fn inner_join_on() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT emp.name, dept.dname FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id",
            &[],
        )
        .await
        .expect("inner join");
    let got: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        got,
        vec![("ann".into(), "eng".into()), ("bob".into(), "sales".into())]
    );
}

#[tokio::test]
async fn left_join_keeps_unmatched_with_nulls() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id",
            &[],
        )
        .await
        .expect("left join");
    let got: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get::<_, Option<String>>(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("ann".into(), Some("eng".into())),
            ("bob".into(), Some("sales".into())),
            ("cy".into(), None),
        ]
    );
}

#[tokio::test]
async fn right_join_keeps_unmatched_dept() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT dept.dname, emp.name FROM emp RIGHT JOIN dept ON emp.dept_id = dept.id ORDER BY dept.id",
            &[],
        )
        .await
        .expect("right join");
    let got: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get::<_, Option<String>>(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("eng".into(), Some("ann".into())),
            ("sales".into(), Some("bob".into())),
            ("ops".into(), None),
        ]
    );
}

#[tokio::test]
async fn full_join_keeps_both_unmatched() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT emp.name, dept.dname FROM emp FULL JOIN dept ON emp.dept_id = dept.id",
            &[],
        )
        .await
        .expect("full join");
    // ann/eng, bob/sales, cy/NULL (unmatched left), NULL/ops (unmatched right).
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn cross_join_is_cartesian() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query("SELECT emp.id, dept.id FROM emp CROSS JOIN dept", &[])
        .await
        .expect("cross join");
    assert_eq!(rows.len(), 9); // 3 x 3
}

#[tokio::test]
async fn comma_join_with_where() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT emp.name FROM emp, dept WHERE emp.dept_id = dept.id ORDER BY emp.id",
            &[],
        )
        .await
        .expect("comma join");
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec!["ann".to_string(), "bob".to_string()]);
}

#[tokio::test]
async fn using_and_natural_merge_join_column() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE l (k int4, lv text)")
        .await
        .expect("l");
    client
        .batch_execute("CREATE TABLE r (k int4, rv text)")
        .await
        .expect("r");
    client
        .batch_execute("INSERT INTO l VALUES (1,'l1'),(2,'l2')")
        .await
        .expect("il");
    client
        .batch_execute("INSERT INTO r VALUES (2,'r2'),(3,'r3')")
        .await
        .expect("ir");
    // USING: merged k first, then lv, then rv.
    let rows = client
        .query("SELECT * FROM l JOIN r USING (k)", &[])
        .await
        .expect("using");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    assert_eq!(rows[0].get::<_, String>(1), "l2");
    assert_eq!(rows[0].get::<_, String>(2), "r2");
    // NATURAL: bare `k` is unambiguous (merged).
    let nat = client
        .query("SELECT k FROM l NATURAL JOIN r", &[])
        .await
        .expect("natural");
    assert_eq!(nat.len(), 1);
    assert_eq!(nat[0].get::<_, i32>(0), 2);
}

#[tokio::test]
async fn qualified_wildcard_expands_one_table() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT emp.* FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id",
            &[],
        )
        .await
        .expect("emp.*");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].len(), 3); // id, name, dept_id
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "ann");
}

#[tokio::test]
async fn self_join_with_aliases() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE node (id int4, parent int4)")
        .await
        .expect("node");
    client
        .batch_execute("INSERT INTO node VALUES (1,NULL),(2,1),(3,2)")
        .await
        .expect("ins");
    let rows = client
        .query(
            "SELECT c.id, p.id FROM node c JOIN node p ON c.parent = p.id ORDER BY c.id",
            &[],
        )
        .await
        .expect("self join");
    let got: Vec<(i32, i32)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(2, 1), (3, 2)]);
}

#[tokio::test]
async fn three_way_join() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE a (id int4)")
        .await
        .expect("a");
    client
        .batch_execute("CREATE TABLE b (id int4, av int4)")
        .await
        .expect("b");
    client
        .batch_execute("CREATE TABLE c (id int4, bv int4)")
        .await
        .expect("c");
    client
        .batch_execute("INSERT INTO a VALUES (1)")
        .await
        .expect("ia");
    client
        .batch_execute("INSERT INTO b VALUES (1, 10)")
        .await
        .expect("ib");
    client
        .batch_execute("INSERT INTO c VALUES (10, 100)")
        .await
        .expect("ic");
    let rows = client
        .query(
            "SELECT c.bv FROM a JOIN b ON a.id = b.id JOIN c ON b.av = c.id",
            &[],
        )
        .await
        .expect("3-way join");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 100);
}

#[tokio::test]
async fn derived_table_in_from() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT d.name FROM (SELECT name, dept_id FROM emp WHERE dept_id IS NOT NULL) d ORDER BY d.name",
            &[],
        )
        .await
        .expect("derived table");
    let got: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec!["ann".to_string(), "bob".to_string()]);
}

#[tokio::test]
async fn aggregate_over_join() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query(
            "SELECT dept.dname, count(*) FROM emp JOIN dept ON emp.dept_id = dept.id GROUP BY dept.dname ORDER BY dept.dname",
            &[],
        )
        .await
        .expect("aggregate over join");
    let got: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![("eng".into(), 1), ("sales".into(), 1)]);
}

#[tokio::test]
async fn left_join_or_equality_branches_return_each_match_once() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE or_left (x int4, y int4); \
             CREATE TABLE or_right (k int4); \
             INSERT INTO or_left \
               SELECT i % 100, CASE WHEN i < 100 THEN i % 100 ELSE (i + 1) % 100 END \
               FROM generate_series(0, 999) AS g(i); \
             INSERT INTO or_right SELECT i % 100 FROM generate_series(0, 999) AS g(i)",
        )
        .await
        .expect("seed OR equijoin");

    let row = client
        .query_one(
            "SELECT count(*) FROM or_left l LEFT JOIN or_right r \
             ON r.k = l.x OR r.k = l.y",
            &[],
        )
        .await
        .expect("OR equijoin count");

    assert_eq!(row.get::<_, i64>(0), 19_000);
}

#[tokio::test]
async fn ambiguous_bare_column_is_42702() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // Both emp and dept have `id`.
    let err = client
        .query("SELECT id FROM emp JOIN dept ON emp.dept_id = dept.id", &[])
        .await
        .expect_err("ambiguous id");
    assert_eq!(err_code(&err), "42702");
}

#[tokio::test]
async fn unknown_column_is_42703() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .query(
            "SELECT emp.nope FROM emp JOIN dept ON emp.dept_id = dept.id",
            &[],
        )
        .await
        .expect_err("unknown column");
    assert_eq!(err_code(&err), "42703");
}

#[tokio::test]
async fn unknown_qualifier_is_42p01() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let err = client
        .query(
            "SELECT zzz.id FROM emp JOIN dept ON emp.dept_id = dept.id",
            &[],
        )
        .await
        .expect_err("unknown qualifier");
    assert_eq!(err_code(&err), "42P01");
}

/// Resolving a column reference is a per-STATEMENT question, so a `WHERE` or
/// `ON` clause naming a column that does not resolve is an error for the
/// statement — over an empty relation, over a populated one, and behind a
/// conjunct that no row satisfies alike.
///
/// This test used to pin the opposite. Binding was best-effort: an unresolvable
/// reference was left as written and reported only when some row evaluated it,
/// which made the DATA decide whether a typo was reported. `AND` short-circuits,
/// so `WHERE a = 99 AND nosuchcol = 1` was silent over a populated table too,
/// and the matching `DELETE` answered `DELETE 0`. `PostgreSQL` reports all of
/// them at parse analysis; the binder now reports them at bind time, which is
/// the same point.
#[tokio::test]
async fn an_unresolvable_reference_is_reported_whether_or_not_a_row_reaches_it() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE l (x int, y int); CREATE TABLE r (x int, z int)")
        .await
        .expect("ddl");
    // (query, the code it reports)
    let cases = [
        ("SELECT 1 FROM l JOIN r ON l.x = r.nope", "42703"),
        ("SELECT 1 FROM l WHERE l.nope = 1", "42703"),
        // Ambiguity is a resolution failure too, and reported just as early.
        ("SELECT 1 FROM l, r WHERE x = 1", "42702"),
        ("SELECT 1 FROM l WHERE zzz.x = 1", "42P01"),
        // The conjunct on the left is false for every row, so evaluation never
        // reached the reference on the right. Binding does.
        ("SELECT 1 FROM l WHERE x = 99 AND nosuchcol = 1", "42703"),
        // A write is the case that mattered: this answered `DELETE 0`.
        ("DELETE FROM l WHERE x = 99 AND nosuchcol = 1", "42703"),
        ("UPDATE l SET y = 1 WHERE x = 99 AND nosuchcol = 1", "42703"),
    ];
    // Every case reports the same code over the empty relation and over the
    // populated one: the data does not decide.
    for populated in [false, true] {
        if populated {
            client
                .batch_execute("INSERT INTO l VALUES (1, 1); INSERT INTO r VALUES (1, 1)")
                .await
                .expect("dml");
        }
        for (sql, code) in cases {
            let err = client
                .query(sql, &[])
                .await
                .expect_err("the reference does not resolve");
            assert!(err_code(&err) == code, "{sql} (populated: {populated})");
        }
    }
}

/// A large equi-join must not degrade to a full inner scan per outer row.
///
/// `pg_regress`'s `join` corpus self-joins a 10k-row table on an equality. If
/// the join folds as a nested loop, it does 100 million predicate evaluations,
/// each one copies a 500-byte text column, and the query never answers. The
/// wall clock is the only thing that separates the two shapes end-to-end. The
/// bound is therefore far above what the indexed probe needs, which is well
/// under a second, and far below what the nested loop needs, which is minutes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_equi_self_join_answers_without_scanning_every_pair() {
    let client = connect(spawn().await).await;
    client
        .simple_query("CREATE TABLE tt3 (f1 int, f2 text)")
        .await
        .expect("create");
    client
        .simple_query(
            "INSERT INTO tt3 SELECT x, repeat('xyzzy', 100) FROM generate_series(1, 10000) x",
        )
        .await
        .expect("seed");

    let rows = tokio::time::timeout(
        std::time::Duration::from_mins(1),
        client.query(
            "SELECT count(*) FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)",
            &[],
        ),
    )
    .await
    .expect("equi-join scanned every pair instead of probing its key")
    .expect("query");

    assert2::assert!(rows[0].get::<_, i64>(0) == 10_000);
}

/// A single-relation `WHERE` conjunct is pre-applied to a *null-preserved* join
/// side so the join walks fewer rows. It must never be pushed to the nullable
/// side: dropping those rows early would suppress the NULL-padded row the outer
/// join owes, and `IS NULL` predicates would then admit rows `PostgreSQL` does
/// not. Every expectation below was taken from `PostgreSQL` 18.4.
#[tokio::test]
async fn a_where_conjunct_pushes_only_into_a_preserved_join_side() {
    let client = connect(spawn().await).await;
    seed(&client).await;

    // (query, expected `name|dname` rows in order)
    let cases: &[(&str, &[&str])] = &[
        // Preserved side: `emp` may be filtered before the join.
        (
            "SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id \
             WHERE emp.id < 3 ORDER BY emp.id",
            &["ann|eng", "bob|sales"],
        ),
        // Nullable side: filtering `dept` early would still be observationally
        // equal here, but the predicate must be applied after padding.
        (
            "SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id \
             WHERE dept.id < 20 ORDER BY emp.id",
            &["ann|eng"],
        ),
        // The case that catches an unsound push: `cy` has a NULL dept_id, so it
        // is NULL-padded and kept. Pushing `IS NULL` onto `dept` would drop
        // every dept row and pad `ann` and `bob` too.
        (
            "SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id \
             WHERE dept.id IS NULL ORDER BY emp.id",
            &["cy|"],
        ),
        // RIGHT JOIN preserves the right side; `emp` is the nullable one.
        (
            "SELECT emp.name, dept.dname FROM emp RIGHT JOIN dept ON emp.dept_id = dept.id \
             WHERE dept.id < 30 ORDER BY dept.id",
            &["ann|eng", "bob|sales"],
        ),
        (
            "SELECT emp.name, dept.dname FROM emp RIGHT JOIN dept ON emp.dept_id = dept.id \
             WHERE emp.id IS NULL ORDER BY dept.id",
            &["|ops"],
        ),
        // FULL JOIN preserves both, so nothing may be pushed.
        (
            "SELECT emp.name, dept.dname FROM emp FULL JOIN dept ON emp.dept_id = dept.id \
             WHERE emp.id IS NULL ORDER BY dept.id",
            &["|ops"],
        ),
    ];

    for (sql, expected) in cases {
        let rows = client.query(*sql, &[]).await.expect(sql);
        let got: Vec<String> = rows
            .iter()
            .map(|r| {
                format!(
                    "{}|{}",
                    r.get::<_, Option<String>>(0).unwrap_or_default(),
                    r.get::<_, Option<String>>(1).unwrap_or_default()
                )
            })
            .collect();
        assert2::assert!(got == *expected, "{sql}");
    }
}
