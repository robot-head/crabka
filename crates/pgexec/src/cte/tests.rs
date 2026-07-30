use assert2::assert;
use crabka_pgwire::engine::{Engine, QueryResult, Session};

use crate::SqlEngine;

async fn rows(setup: &[&str], sql: &str) -> Vec<Vec<Option<String>>> {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for statement in setup {
        session.simple_query(statement).await.expect("setup");
    }
    let results = session.simple_query(sql).await.expect("query");
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows from {sql}")
    };
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref()
                        .map(|c| String::from_utf8(c.text.to_vec()).expect("utf-8"))
                })
                .collect()
        })
        .collect()
}

async fn sqlstate(setup: &[&str], sql: &str) -> String {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for statement in setup {
        session.simple_query(statement).await.expect("setup");
    }
    session
        .simple_query(sql)
        .await
        .expect_err("expected an error")
        .code
}

fn column(rows: Vec<Vec<Option<String>>>) -> Vec<String> {
    rows.into_iter()
        .map(|row| row[0].clone().unwrap_or_else(|| "NULL".into()))
        .collect()
}

const TREE: &[&str] = &[
    "CREATE TABLE tree (id int4, parent int4)",
    "INSERT INTO tree VALUES (1,NULL),(2,1),(3,1),(4,2),(5,3)",
];

#[tokio::test]
async fn recursive_union_all_counts_up() {
    let got = rows(
        &[],
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) \
         SELECT n FROM t ORDER BY n",
    )
    .await;
    assert!(column(got) == ["1", "2", "3", "4", "5"]);
}

#[tokio::test]
async fn recursive_union_drops_rows_already_produced() {
    // Without the duplicate check this step cycles 1 -> 2 -> 0 -> 1 forever.
    let got = rows(
        &[],
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT (n + 1) % 3 FROM t) \
         SELECT n FROM t ORDER BY n",
    )
    .await;
    assert!(column(got) == ["0", "1", "2"]);
}

#[tokio::test]
async fn recursive_walk_over_a_table() {
    let got = rows(
        TREE,
        "WITH RECURSIVE walk(id, depth) AS ( \
           SELECT id, 0 FROM tree WHERE parent IS NULL \
           UNION ALL \
           SELECT c.id, w.depth + 1 FROM tree c JOIN walk w ON c.parent = w.id) \
         SELECT id, depth FROM walk ORDER BY depth, id",
    )
    .await;
    let flat: Vec<(String, String)> = got
        .into_iter()
        .map(|row| (row[0].clone().expect("id"), row[1].clone().expect("depth")))
        .collect();
    assert!(
        flat == [
            ("1".into(), "0".into()),
            ("2".into(), "1".into()),
            ("3".into(), "1".into()),
            ("4".into(), "2".into()),
            ("5".into(), "2".into()),
        ]
    );
}

#[tokio::test]
async fn non_recursive_term_may_itself_be_a_union() {
    let got = rows(
        &[],
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION ALL SELECT 10 UNION ALL SELECT n + 100 FROM t WHERE n < 100) \
         SELECT n FROM t ORDER BY n",
    )
    .await;
    assert!(column(got) == ["1", "10", "101", "110"]);
}

#[tokio::test]
async fn recursive_item_beside_and_before_a_plain_item() {
    let got = rows(
        &[],
        "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 4), \
              doubled AS (SELECT n * 2 AS d FROM nums) \
         SELECT d FROM doubled ORDER BY d",
    )
    .await;
    assert!(column(got) == ["2", "4", "6", "8"]);
}

#[tokio::test]
async fn recursive_list_is_reordered_so_a_dependency_runs_first() {
    // `nums` reads `seed`, which is written after it: WITH RECURSIVE sorts.
    let got = rows(
        &[],
        "WITH RECURSIVE nums(n) AS (SELECT s FROM seed UNION ALL SELECT n + 1 FROM nums WHERE n < 4), \
              seed AS (SELECT 2 AS s) \
         SELECT n FROM nums ORDER BY n",
    )
    .await;
    assert!(column(got) == ["2", "3", "4"]);
}

#[tokio::test]
async fn materialized_hints_are_accepted_and_change_nothing() {
    for hint in ["", "MATERIALIZED ", "NOT MATERIALIZED "] {
        let sql = format!(
            "WITH a AS {hint}(SELECT 1 AS x UNION ALL SELECT 2) SELECT x FROM a ORDER BY x"
        );
        assert!(column(rows(&[], &sql).await) == ["1", "2"], "{sql}");
    }
}

#[tokio::test]
async fn recursive_cte_inside_a_subquery() {
    let got = rows(
        &[],
        "SELECT (WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 4) \
                 SELECT sum(n) FROM t)",
    )
    .await;
    assert!(column(got) == ["10"]);
}

#[tokio::test]
async fn recursive_errors_use_postgres_sqlstates() {
    let cases = [
        // Not a UNION at all.
        (
            "WITH RECURSIVE t(n) AS (SELECT n FROM t) SELECT n FROM t",
            "42P19",
        ),
        // Set operator other than UNION.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 INTERSECT SELECT n FROM t) SELECT n FROM t",
            "42P19",
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 EXCEPT SELECT n FROM t) SELECT n FROM t",
            "42P19",
        ),
        // Self-reference in the non-recursive term.
        (
            "WITH RECURSIVE t(n) AS (SELECT n + 1 FROM t UNION ALL SELECT 1) SELECT n FROM t",
            "42P19",
        ),
        // More than one self-reference.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT x.n + y.n FROM t x, t y WHERE x.n < 3) \
             SELECT n FROM t",
            "42P19",
        ),
        // Aggregate in the recursive term.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT count(*) FROM t) SELECT n FROM t",
            "42P19",
        ),
        // Self-reference inside a subquery.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT (SELECT max(n) FROM t) + 1) \
             SELECT n FROM t",
            "42P19",
        ),
        // Mutual recursion.
        (
            "WITH RECURSIVE x(i) AS (SELECT 1 UNION ALL SELECT i FROM y), \
                  y(i) AS (SELECT 1 UNION ALL SELECT i FROM x) SELECT i FROM x",
            "0A000",
        ),
        // A result-level tail on the recursion itself.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 LIMIT 2) \
             SELECT n FROM t",
            "0A000",
        ),
        // Mismatched column counts across the recursion.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1, 2 FROM t WHERE n < 3) \
             SELECT n FROM t",
            "42601",
        ),
        // A forward reference in a plain WITH is an unknown relation.
        (
            "WITH x(i) AS (SELECT * FROM y), y(i) AS (SELECT 1) SELECT i FROM x",
            "42P01",
        ),
        // Column alias count mismatch.
        ("WITH a(x, y) AS (SELECT 1) SELECT x FROM a", "42P10"),
        // SEARCH / CYCLE on a non-recursive item.
        (
            "WITH t AS (SELECT 1 AS n) SEARCH BREADTH FIRST BY n SET seq SELECT n FROM t",
            "42601",
        ),
        (
            "WITH t AS (SELECT 1 AS n) CYCLE n SET is_cycle USING path SELECT n FROM t",
            "42601",
        ),
    ];
    for (sql, code) in cases {
        assert!(sqlstate(&[], sql).await == code, "{sql}");
    }
}

/// `PostgreSQL` scopes its "no aggregate in the recursive term" rule to the
/// query level that HOLDS the self-reference, not to the recursive term as a
/// whole (`checkWellFormedRecursion` descends into a FROM-clause sub-SELECT and
/// judges that level's own aggregation). So an aggregate one level above a
/// self-reference is legal, and one at the self-reference's own level is not,
/// however deeply nested that level is.
#[tokio::test]
async fn the_recursive_term_aggregate_rule_is_scoped_to_the_self_references_own_level() {
    let legal = [
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT max(n) + 1 FROM (SELECT n FROM t WHERE n < 3) q) \
         SELECT n FROM t ORDER BY 1",
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT sum(n)::int + 1 FROM (SELECT n FROM t WHERE n < 3) q) \
         SELECT n FROM t ORDER BY 1",
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT count(*)::int + n FROM (SELECT n FROM t WHERE n < 3) q GROUP BY n) \
         SELECT n FROM t ORDER BY 1",
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT max(n)::int + 1 FROM (SELECT n FROM (SELECT n FROM t WHERE n < 3) r) q) \
         SELECT n FROM t ORDER BY 1",
        // GROUP BY and DISTINCT at the self-reference's own level are not
        // aggregation for this rule.
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT n + 1 FROM (SELECT n FROM t WHERE n < 3 GROUP BY n) q) \
         SELECT n FROM t ORDER BY 1",
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT n + 1 FROM (SELECT DISTINCT n FROM t WHERE n < 3) q) \
         SELECT n FROM t ORDER BY 1",
    ];
    for sql in legal {
        let got = rows(&[], sql).await;
        assert!(column(got)[..3] == ["1", "2", "3"], "{sql}");
    }

    let refused = [
        // Aggregate in the select list of the level holding the self-reference.
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT n + 1 FROM (SELECT max(n) AS n FROM t WHERE n < 3) q \
          WHERE n IS NOT NULL) \
         SELECT n FROM t",
        // …in that level's HAVING.
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT n + 1 FROM \
          (SELECT n FROM t WHERE n < 3 GROUP BY n HAVING count(*) > 0) q) \
         SELECT n FROM t",
        // …two levels down.
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION SELECT n + 1 FROM \
          (SELECT n FROM (SELECT max(n) AS n FROM t WHERE n < 3) r) q WHERE n IS NOT NULL) \
         SELECT n FROM t",
        // …and at the recursive term's own top level, which is that level too.
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT max(n) + 1 FROM t WHERE n < 3) \
         SELECT n FROM t",
    ];
    for sql in refused {
        assert!(sqlstate(&[], sql).await == "42P19", "{sql}");
    }
}

#[tokio::test]
async fn self_reference_on_the_nullable_side_of_an_outer_join_is_rejected() {
    let code = sqlstate(
        TREE,
        "WITH RECURSIVE t(n) AS \
         (SELECT 1 UNION ALL SELECT t.n + 1 FROM tree LEFT JOIN t ON true WHERE t.n < 3) \
         SELECT n FROM t",
    )
    .await;
    assert!(code == "42P19");
}

/// A column alias list SHORTER than the query is legal: the trailing columns
/// keep the names the query gave them. Only a longer list is 42P10.
#[tokio::test]
async fn a_short_column_alias_list_names_only_the_leading_columns() {
    let cases: &[(&str, &[&str])] = &[
        (
            "WITH t(a) AS (SELECT 1 AS x, 2 AS y) SELECT a FROM t",
            &["1"],
        ),
        (
            "WITH t(a) AS (SELECT 1 AS x, 2 AS y) SELECT y FROM t",
            &["2"],
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1, 100 UNION ALL SELECT n + 1, 200 FROM t \
             WHERE n < 3) SELECT n FROM t ORDER BY n",
            &["1", "2", "3"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(column(rows(&[], sql).await) == *expected, "{sql}");
    }
    // Two columns available, three named.
    assert!(
        sqlstate(
            &[],
            "WITH t(a, b, c) AS (SELECT 1 AS x, 2 AS y) SELECT * FROM t"
        )
        .await
            == "42P10"
    );
    // With an alias list present, a UNION column-count mismatch is still 42601 —
    // the alias check must not fire first and mask it.
    assert!(
        sqlstate(
            &[],
            "WITH RECURSIVE t(n) AS (SELECT 1, 2 UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
             SELECT n FROM t"
        )
        .await
            == "42601"
    );
}

/// `PostgreSQL` type-checks the recursive term at analysis time: the
/// non-recursive term fixes each column's type and the UNION's common type must
/// equal it. Both failures are 42804, raised before a single round runs.
#[tokio::test]
async fn a_recursive_term_that_changes_a_column_type_is_42804() {
    let cases: &[(&str, &str)] = &[
        (
            "WITH RECURSIVE t(n) AS (SELECT 1::int UNION ALL SELECT 1.5 FROM t WHERE n < 3) \
             SELECT n FROM t",
            "recursive query \"t\" column 1 has type integer in non-recursive term but type \
             numeric overall",
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1::int2 UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
             SELECT n FROM t",
            "recursive query \"t\" column 1 has type smallint in non-recursive term but type \
             integer overall",
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n::text FROM t WHERE n < 3) \
             SELECT n FROM t",
            "UNION types integer and text cannot be matched",
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT 'x' || n FROM t WHERE n < 3) \
             SELECT n FROM t",
            "UNION types integer and text cannot be matched",
        ),
    ];
    let engine = SqlEngine::new();
    for (sql, message) in cases {
        let mut session = engine.connect();
        let error = session.simple_query(sql).await.expect_err(sql);
        assert!(error.code == "42804", "{sql}");
        assert!(error.message == *message, "{sql}");
    }
    // A narrowing recursive term is fine — the common type is the seeded one.
    assert!(
        column(
            rows(
                &[],
                "WITH RECURSIVE t(n) AS (SELECT 1::int8 UNION ALL SELECT (n + 1)::int4 FROM t \
                 WHERE n < 3) SELECT n FROM t ORDER BY n"
            )
            .await
        ) == ["1", "2", "3"]
    );
    // A bare literal is PostgreSQL's `unknown` and adopts the seeded type, so
    // this is well-typed and fails at run time on the cast instead.
    assert!(
        sqlstate(
            &[],
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM t) SELECT n FROM t"
        )
        .await
            == "22P02"
    );
}

/// A FROM-clause sub-SELECT is not a "subquery" for the self-reference rule —
/// `PostgreSQL` restricts SubLinks, not derived tables.
#[tokio::test]
async fn a_self_reference_inside_a_from_clause_subselect_is_allowed() {
    let cases: &[(&str, &[&str])] = &[
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT n FROM t) q \
             WHERE n < 3) SELECT n FROM t ORDER BY n",
            &["1", "2", "3"],
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM \
             (SELECT n FROM t UNION ALL SELECT 9) q WHERE n < 3) SELECT n FROM t ORDER BY n",
            &["1", "2", "3"],
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM \
             (WITH x AS (SELECT n FROM t) SELECT n FROM x) q WHERE n < 3) \
             SELECT n FROM t ORDER BY n",
            &["1", "2", "3"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(column(rows(&[], sql).await) == *expected, "{sql}");
    }
}

/// The rules a derived table does NOT escape: an expression subquery is still
/// 42P19, the reference is still counted for the once-only rule, and the
/// nullable side of an outer join is still refused.
#[tokio::test]
async fn a_derived_table_does_not_escape_the_remaining_recursion_rules() {
    let cases: &[&str] = &[
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t \
         WHERE n < (SELECT max(n) FROM t)) SELECT n FROM t",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM \
         (SELECT n FROM t WHERE EXISTS (SELECT 1 FROM t)) q WHERE n < 3) SELECT n FROM t",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT q.n + 1 FROM t, \
         (SELECT n FROM t) q WHERE t.n < 3) SELECT n FROM t",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT q.n + 1 FROM tree \
         LEFT JOIN (SELECT n FROM t) q ON true WHERE q.n < 3) SELECT n FROM t",
    ];
    for sql in cases {
        assert!(sqlstate(TREE, sql).await == "42P19", "{sql}");
    }
}
