//! A whole-row reference to the null-extended side of an outer join.
//!
//! `SELECT jb FROM ja LEFT JOIN jb ON …` names a row the join invented, and that
//! reference is NULL *as a whole* — `count(jb)` skips it, `jb::text` renders
//! nothing. A stored row whose every column happens to be NULL is a different
//! value: an ordinary composite that renders `(,)` and is counted.
//!
//! Nothing about the two values separates them. `IS NULL` is true for both,
//! because on a composite it is field-wise; and `ja LEFT JOIN jb ON true` puts a
//! stored all-NULL row and an invented one in the same result, so the query
//! shape cannot separate them either. Only where the row came from can, which is
//! why the join marks the side it invents and [`crabka_pgexec`] carries that
//! marker out to the projection.
//!
//! Every expectation here was read off `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// Every row, each as its cells joined by `|`, with a NULL cell as `<NULL>` —
/// which is what tells a NULL whole row from the `(,)` an all-NULL composite
/// renders.
async fn rows(s: &mut SqlSession, sql: &str) -> Vec<String> {
    match &s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))[0]
    {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|c: &Option<Cell>| {
                        c.as_ref().map_or_else(
                            || "<NULL>".to_string(),
                            |c| String::from_utf8_lossy(&c.text).into_owned(),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn fixture(s: &mut SqlSession) {
    for sql in [
        "CREATE TABLE ja (x int, y text)",
        "CREATE TABLE jb (x int, z text)",
        "CREATE TABLE jc (x int, w text)",
        "CREATE TABLE jempty (x int, q text)",
        "CREATE TABLE jone (k int)",
        "INSERT INTO ja VALUES (1,'a'),(2,'b')",
        "INSERT INTO jb VALUES (1,'p')",
        "INSERT INTO jc VALUES (1,'r')",
        "INSERT INTO jone VALUES (1),(9)",
    ] {
        s.simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

async fn check(s: &mut SqlSession, cases: &[(&str, &[&str])]) {
    for (sql, expected) in cases {
        let got = rows(s, sql).await;
        let want: Vec<String> = expected.iter().map(|e| (*e).to_string()).collect();
        assert!(got == want, "{sql}");
    }
}

/// The reference is NULL, and so is everything computed from it: the cast, the
/// JSON, the field selection, the aggregate that skips NULLs, the group key, the
/// sort key and the equality.
#[tokio::test]
async fn a_whole_row_reference_to_a_null_extended_side_is_null() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    check(
        &mut s,
        &[
            // The defect in one line: the invented row must not be counted.
            (
                "SELECT count(jb) FROM ja LEFT JOIN jb ON ja.x = jb.x",
                &["1"],
            ),
            (
                "SELECT ja.x, jb::text, jb IS NULL, jb IS NOT NULL \
                 FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
                &["1|(1,p)|f|t", "2|<NULL>|t|f"],
            ),
            (
                "SELECT row_to_json(jb) FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY ja.x",
                &[r#"{"x":1,"z":"p"}"#, "<NULL>"],
            ),
            // A field of a NULL composite is NULL, not an error.
            (
                "SELECT (jb).x, (jb).z FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY ja.x",
                &["1|p", "<NULL>|<NULL>"],
            ),
            (
                "SELECT COALESCE(jb::text, 'none') FROM ja LEFT JOIN jb ON ja.x = jb.x \
                 ORDER BY ja.x",
                &["(1,p)", "none"],
            ),
            // Equality with a NULL operand is NULL, and the value is not
            // distinct from NULL — the two tests a stored all-NULL row answers
            // the other way round.
            (
                "SELECT ja.x, jb = jb, jb IS DISTINCT FROM NULL \
                 FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
                &["1|t|t", "2|<NULL>|f"],
            ),
            (
                "SELECT jb::text, count(*) FROM ja LEFT JOIN jb ON ja.x = jb.x \
                 GROUP BY jb ORDER BY 1",
                &["(1,p)|1", "<NULL>|1"],
            ),
            (
                "SELECT DISTINCT jb::text FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
                &["(1,p)", "<NULL>"],
            ),
            (
                "SELECT ja.x FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY jb NULLS FIRST",
                &["2", "1"],
            ),
            (
                "SELECT array_agg(jb::text ORDER BY ja.x) FROM ja LEFT JOIN jb ON ja.x = jb.x",
                &[r#"{"(1,p)",NULL}"#],
            ),
            // A whole-row reference outside the select list.
            (
                "SELECT ja.x FROM ja LEFT JOIN jb ON ja.x = jb.x WHERE jb IS NULL",
                &["2"],
            ),
            (
                "SELECT ja.x FROM ja LEFT JOIN jb ON ja.x = jb.x \
                 GROUP BY ja.x, jb HAVING count(jb) = 0",
                &["2"],
            ),
            // A side that matches nothing at all is still invented per row.
            (
                "SELECT ja.x, e::text, count(e) OVER () FROM ja LEFT JOIN jempty e \
                 ON e.x = ja.x ORDER BY 1",
                &["1|<NULL>|0", "2|<NULL>|0"],
            ),
        ],
    )
    .await;
}

/// Every reader that treated the invented row as a composite because it could
/// not tell the difference. Each of these was right only by accident before, and
/// each now follows from the value simply being NULL.
#[tokio::test]
async fn the_readers_that_could_not_tell_the_difference_all_see_a_null() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    check(
        &mut s,
        &[
            // Aggregates that skip NULLs skip it, including DISTINCT counting.
            (
                "SELECT count(jb), count(DISTINCT jb), min(jb::text) \
                 FROM ja LEFT JOIN jb ON ja.x = jb.x",
                &["1|1|(1,p)"],
            ),
            // Aggregates that keep NULLs keep it as a NULL element, not as a
            // composite of NULLs. (`array_agg(jb)` itself is out of reach here:
            // an array of `record` is unimplemented, which predates this and is
            // unrelated to it.)
            (
                "SELECT json_agg(jb ORDER BY ja.x)::text FROM ja LEFT JOIN jb ON ja.x = jb.x",
                &[r#"[{"x":1,"z":"p"}, null]"#],
            ),
            (
                "SELECT to_json(jb)::text FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY ja.x",
                &[r#"{"x":1,"z":"p"}"#, "<NULL>"],
            ),
            // `IS NOT DISTINCT FROM NULL` is the test `IS NULL` cannot make: it
            // is true only for the invented row.
            (
                "SELECT ja.x FROM ja LEFT JOIN jb ON ja.x = jb.x \
                 WHERE jb IS NOT DISTINCT FROM NULL ORDER BY 1",
                &["2"],
            ),
            // Default sort places it last, as a NULL.
            (
                "SELECT ja.x FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY jb",
                &["1", "2"],
            ),
        ],
    )
    .await;
}

/// A stored row of NULLs is not the same value, and the difference cannot be
/// read off the row: `ON true` matches the stored all-NULL row, so one result
/// holds both an invented row and a matched one whose columns are identical.
#[tokio::test]
async fn a_stored_all_null_row_is_a_composite_not_a_null() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;
    s.simple_query("INSERT INTO jb VALUES (NULL, NULL)")
        .await
        .expect("insert");

    check(
        &mut s,
        &[
            // `IS NULL` agrees with the invented row; `IS DISTINCT FROM NULL`
            // does not, which is what makes them two values.
            (
                "SELECT jb::text, jb IS NULL, jb IS DISTINCT FROM NULL FROM jb \
                 ORDER BY x NULLS LAST",
                &["(1,p)|f|t", "(,)|t|t"],
            ),
            ("SELECT count(jb) FROM jb", &["2"]),
            // Both stored rows match `ON true`, so none of these four rows was
            // invented and none is NULL.
            (
                "SELECT ja.x, jb::text, jb IS DISTINCT FROM NULL \
                 FROM ja LEFT JOIN jb ON true ORDER BY 1, 2",
                &["1|(,)|t", "1|(1,p)|t", "2|(,)|t", "2|(1,p)|t"],
            ),
            (
                "SELECT count(jb), count(*) FROM ja LEFT JOIN jb ON true",
                &["4|4"],
            ),
            (
                "SELECT jb::text, count(*) FROM ja LEFT JOIN jb ON true GROUP BY jb ORDER BY 1",
                &["(,)|2", "(1,p)|2"],
            ),
        ],
    )
    .await;
}

/// A row constructor over the very same columns stays a composite: `ROW(jb.x,
/// jb.z)` is `(,)` where `jb` is NULL. The two are built from one row and differ,
/// so the marker is what separates them, not the values.
#[tokio::test]
async fn a_row_constructor_over_the_invented_columns_is_still_a_composite() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    check(
        &mut s,
        &[(
            "SELECT ja.x, ROW(jb.x, jb.z)::text, jb::text \
             FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
            &["1|(1,p)|(1,p)", "2|(,)|<NULL>"],
        )],
    )
    .await;
}

/// Every join shape that can invent a row: the right side of a LEFT, the left of
/// a RIGHT, either of a FULL, a `USING` join's nullable side, a LATERAL one, a
/// derived table, a CTE, and an already-invented row invented again by a join
/// above it.
#[tokio::test]
async fn every_shape_that_null_extends_marks_the_side_it_invents() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    check(
        &mut s,
        &[
            // RIGHT: it is the LEFT input that gets invented.
            (
                "SELECT count(jb) FROM jb RIGHT JOIN ja ON ja.x = jb.x",
                &["1"],
            ),
            (
                "SELECT jb.x, ja::text, ja IS NULL FROM jb RIGHT JOIN ja ON ja.x = jb.x \
                 ORDER BY 2",
                &["1|(1,a)|f", "<NULL>|(2,b)|f"],
            ),
            // FULL: either side.
            (
                "SELECT ja::text, jb::text FROM ja FULL JOIN jb ON ja.x = jb.x ORDER BY 1, 2",
                &["(1,a)|(1,p)", "(2,b)|<NULL>"],
            ),
            // USING keeps both sides' raw columns and a merged one; the raw
            // side still has a whole row, and it is the invented one.
            (
                "SELECT x, jb::text FROM ja LEFT JOIN jb USING (x) ORDER BY 1",
                &["1|(1,p)", "2|<NULL>"],
            ),
            (
                "SELECT x, ja::text, jb::text FROM ja FULL JOIN jb USING (x) ORDER BY 1",
                &["1|(1,a)|(1,p)", "2|(2,b)|<NULL>"],
            ),
            // Nested: the inner join invents `jc`, the outer invents both.
            (
                "SELECT ja.x, jb::text, jc::text \
                 FROM ja LEFT JOIN (jb LEFT JOIN jc ON jb.x = jc.x) ON ja.x = jb.x ORDER BY 1",
                &["1|(1,p)|(1,r)", "2|<NULL>|<NULL>"],
            ),
            // An already-invented row invented again one level up.
            (
                "SELECT jone.k, jb::text FROM jone LEFT JOIN (ja LEFT JOIN jb ON ja.x = jb.x) \
                 ON ja.x = jone.k ORDER BY 1",
                &["1|(1,p)", "9|<NULL>"],
            ),
            // A derived table and a CTE are range-table entries like any other.
            (
                "SELECT ja.x, s::text FROM ja LEFT JOIN (SELECT * FROM jb) s ON ja.x = s.x \
                 ORDER BY 1",
                &["1|(1,p)", "2|<NULL>"],
            ),
            (
                "WITH cte AS (SELECT * FROM jb) \
                 SELECT ja.x, cte::text FROM ja LEFT JOIN cte ON ja.x = cte.x ORDER BY 1",
                &["1|(1,p)", "2|<NULL>"],
            ),
            (
                "SELECT ja.x, l::text FROM ja LEFT JOIN LATERAL \
                 (SELECT * FROM jb WHERE jb.x = ja.x) l ON true ORDER BY 1",
                &["1|(1,p)", "2|<NULL>"],
            ),
            // A derived table whose whole self is invented.
            (
                "SELECT jone.k, j::text FROM jone LEFT JOIN \
                 (SELECT ja.x AS ax, ja.y, jb.z FROM ja JOIN jb ON ja.x = jb.x) AS j \
                 ON j.ax = jone.k ORDER BY 1",
                &["1|(1,a,p)", "9|<NULL>"],
            ),
            // Projected into a column, it is an ordinary NULL from then on.
            (
                "SELECT s.r::text FROM \
                 (SELECT jb AS r FROM ja LEFT JOIN jb ON ja.x = jb.x) s ORDER BY 1",
                &["(1,p)", "<NULL>"],
            ),
        ],
    )
    .await;
}

/// The marker rides in the row and the scope alike, so it must stay invisible to
/// everything that reads either — and a query with no outer join in it must be
/// bit-for-bit what it was.
#[tokio::test]
async fn the_marker_is_invisible_to_every_other_reader() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    check(
        &mut s,
        &[
            // `*` expands to the relation's columns and no more, over an outer
            // join, a USING outer join, and a qualified star.
            (
                "SELECT * FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
                &["1|a|1|p", "2|b|<NULL>|<NULL>"],
            ),
            (
                "SELECT * FROM ja LEFT JOIN jb USING (x) ORDER BY 1",
                &["1|a|p", "2|b|<NULL>"],
            ),
            (
                "SELECT ja.* FROM ja LEFT JOIN jb USING (x) ORDER BY 1",
                &["1|a", "2|b"],
            ),
            (
                "SELECT * FROM ja FULL JOIN jb USING (x) ORDER BY 1",
                &["1|a|p", "2|b|<NULL>"],
            ),
            (
                "SELECT count(*) FROM ja LEFT JOIN jb ON ja.x = jb.x",
                &["2"],
            ),
            // A column reference resolves to the same index it always did.
            (
                "SELECT ja.x, jb.x FROM ja LEFT JOIN jb ON ja.x = jb.x ORDER BY 1",
                &["1|1", "2|<NULL>"],
            ),
            // No outer join anywhere: nothing is marked and nothing changes.
            ("SELECT ja::text FROM ja ORDER BY x", &["(1,a)", "(2,b)"]),
            ("SELECT count(ja) FROM ja", &["2"]),
            (
                "SELECT ja.x, jb::text FROM ja JOIN jb ON ja.x = jb.x",
                &["1|(1,p)"],
            ),
            (
                "SELECT ja::text, jb::text FROM ja CROSS JOIN jb ORDER BY 1",
                &["(1,a)|(1,p)", "(2,b)|(1,p)"],
            ),
            // A derived table of NULL literals is a composite, not a NULL: it
            // was selected, not invented.
            (
                "SELECT s::text, s IS NULL FROM (SELECT NULL::int AS x, NULL::text AS z) s",
                &["(,)|t"],
            ),
            // Last, because it writes: an outer join under UPDATE … FROM, where
            // the target's columns are prepended to the source's scope and to
            // its row alike.
            (
                "UPDATE ja SET y = 'u' FROM ja a2 LEFT JOIN jb ON a2.x = jb.x \
                 WHERE ja.x = a2.x RETURNING ja.x, ja.y",
                &["1|u", "2|u"],
            ),
        ],
    )
    .await;
}

/// Joining a relation to itself under one name is still a duplicate alias — the
/// check that rejects it now has to skip the marker columns, whose qualifier is
/// shared by both sides on purpose.
#[tokio::test]
async fn duplicate_aliases_are_still_rejected_across_nested_outer_joins() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    fixture(&mut s).await;

    for sql in [
        "SELECT 1 FROM ja LEFT JOIN ja ON true",
        "SELECT 1 FROM (ja LEFT JOIN jb ON ja.x = jb.x) LEFT JOIN jb ON true",
    ] {
        let error = s
            .simple_query(sql)
            .await
            .expect_err("expected a duplicate alias")
            .to_string();
        assert!(error.contains("42712"), "{sql}: {error}");
    }

    // Two outer joins each marking their own side, under one more that marks
    // both: the markers coexist and the join is accepted.
    check(
        &mut s,
        &[(
            "SELECT jone.k, jb::text, jc::text FROM jone \
             LEFT JOIN (ja LEFT JOIN jb ON ja.x = jb.x) ON ja.x = jone.k \
             LEFT JOIN jc ON jc.x = jone.k ORDER BY 1",
            &["1|(1,p)|(1,r)", "9|<NULL>|<NULL>"],
        )],
    )
    .await;
}
