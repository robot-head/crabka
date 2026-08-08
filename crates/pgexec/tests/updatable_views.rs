//! Writing through a view that carries no `INSTEAD OF` trigger.
//!
//! `PostgreSQL` rewrites an `INSERT`/`UPDATE`/`DELETE` that names a simple
//! enough view onto the relation underneath it. Three things about that are
//! worth pinning down, and this file is organised around them.
//!
//! * **Which views qualify, and what a refusal says.** The `DETAIL` naming the
//!   disqualifying clause is the only thing that tells a user why their view is
//!   read-only, so every clause is asserted by its own message rather than by a
//!   yes/no.
//! * **What the rewrite does with the statement.** Renamed columns, computed
//!   columns that refuse assignment, unselected columns taking the base
//!   relation's default, `RETURNING` keeping the view's column names, and the
//!   view's own qualification restricting which rows a write can reach.
//! * **`WITH CHECK OPTION`.** Whose qualification is enforced, in what order,
//!   and where a cascade stops.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match run(session, sql).await.pop().expect("one result") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The output column names of the last statement's result.
async fn headers(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match run(session, sql).await.pop().expect("one result") {
        QueryResult::Rows { fields, .. } => fields.iter().map(|field| field.name.clone()).collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// A failure's SQLSTATE, message, `DETAIL` and `HINT` — the four things
/// `PostgreSQL` distinguishes a view refusal by.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
}

impl Failure {
    fn new(code: &str, message: &str, detail: Option<&str>, hint: Option<&str>) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            detail: detail.map(ToString::to_string),
            hint: hint.map(ToString::to_string),
        }
    }
}

async fn failure(session: &mut SqlSession, sql: &str) -> Failure {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    Failure {
        code: error.code.clone(),
        message: error.message.clone(),
        detail: error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.clone()),
        hint: error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.hint.clone()),
    }
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

async fn engine_with(setup: &str) -> (SqlEngine, Arc<dyn Kv>) {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    let mut session = engine.connect();
    run(&mut session, setup).await;
    (engine, kv)
}

const BASE: &str = "CREATE TABLE base_tbl (a int, b text DEFAULT 'D', c int);
     INSERT INTO base_tbl VALUES (1,'r1',10),(2,'r2',20),(3,'r3',30);";

/// The `HINT` each write spells, which names the two things `PostgreSQL` would
/// accept in place of a rewrite.
const INSERT_HINT: &str = "To enable inserting into the view, provide an INSTEAD OF INSERT trigger \
                           or an unconditional ON INSERT DO INSTEAD rule.";
const UPDATE_HINT: &str = "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an \
                           unconditional ON UPDATE DO INSTEAD rule.";
const DELETE_HINT: &str = "To enable deleting from the view, provide an INSTEAD OF DELETE trigger \
                           or an unconditional ON DELETE DO INSTEAD rule.";

/// Each case is a view body, and the `DETAIL` a write through it reports.
///
/// Every clause `PostgreSQL`'s `view_query_is_auto_updatable` tests is here,
/// because the reason it gives is the whole value of the refusal — a bare "not
/// updatable" leaves a user with no way to find the clause at fault.
#[tokio::test]
async fn a_disqualifying_clause_names_itself_in_the_refusal() {
    let cases: [(&str, &str); 16] = [
        (
            "SELECT DISTINCT a, b FROM base_tbl",
            "Views containing DISTINCT are not automatically updatable.",
        ),
        (
            "SELECT a, b FROM base_tbl GROUP BY a, b",
            "Views containing GROUP BY are not automatically updatable.",
        ),
        (
            "SELECT 1 FROM base_tbl HAVING max(a) > 0",
            "Views containing HAVING are not automatically updatable.",
        ),
        (
            "SELECT count(*) FROM base_tbl",
            "Views that return aggregate functions are not automatically updatable.",
        ),
        (
            "SELECT a, rank() OVER () FROM base_tbl",
            "Views that return window functions are not automatically updatable.",
        ),
        (
            "SELECT a, b FROM base_tbl UNION SELECT -a, b FROM base_tbl",
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.",
        ),
        (
            "WITH t AS (SELECT a, b FROM base_tbl) SELECT * FROM t",
            "Views containing WITH are not automatically updatable.",
        ),
        (
            "SELECT a, b FROM base_tbl ORDER BY a OFFSET 1",
            "Views containing LIMIT or OFFSET are not automatically updatable.",
        ),
        (
            "SELECT a, b FROM base_tbl ORDER BY a LIMIT 1",
            "Views containing LIMIT or OFFSET are not automatically updatable.",
        ),
        (
            "SELECT a, b, generate_series(1, a) g FROM base_tbl",
            "Views that return set-returning functions are not automatically updatable.",
        ),
        (
            "SELECT 1 AS a",
            "Views that do not select from a single table or view are not automatically updatable.",
        ),
        (
            "SELECT b1.a, b2.b FROM base_tbl b1, base_tbl b2",
            "Views that do not select from a single table or view are not automatically updatable.",
        ),
        (
            "SELECT * FROM generate_series(1, 10) AS g(a)",
            "Views that do not select from a single table or view are not automatically updatable.",
        ),
        (
            "SELECT a, b FROM (SELECT * FROM base_tbl) AS t",
            "Views that do not select from a single table or view are not automatically updatable.",
        ),
        (
            "SELECT * FROM (VALUES (1)) AS tmp(a)",
            "Views that do not select from a single table or view are not automatically updatable.",
        ),
        (
            "SELECT upper(b) AS u FROM base_tbl",
            "Views that have no updatable columns are not automatically updatable.",
        ),
    ];
    let (engine, _kv) = engine_with(BASE).await;
    let mut session = engine.connect();
    for (index, (body, detail)) in cases.into_iter().enumerate() {
        let view = format!("ro{index}");
        run(&mut session, &format!("CREATE VIEW {view} AS {body}")).await;
        assert!(
            failure(&mut session, &format!("UPDATE {view} SET a = 1")).await
                == Failure::new(
                    "55000",
                    &format!("cannot update view \"{view}\""),
                    Some(detail),
                    Some(UPDATE_HINT),
                ),
            "{body}"
        );
    }
}

/// `DELETE` is the one write that does not need an updatable column, so a view
/// whose every column is computed still admits it.
#[tokio::test]
async fn delete_does_not_require_an_updatable_column() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW v AS SELECT upper(b) AS u FROM base_tbl WHERE a > 1"
    ))
    .await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "INSERT INTO v VALUES ('X')").await
            == Failure::new(
                "55000",
                "cannot insert into view \"v\"",
                Some("Views that have no updatable columns are not automatically updatable."),
                Some(INSERT_HINT),
            )
    );
    run(&mut session, "DELETE FROM v").await;
    assert!(query(&mut session, "SELECT a FROM base_tbl ORDER BY a").await == rows(&["1"]));
}

/// Each write spells its own refusal, and `PostgreSQL` does not derive one from
/// another: "insert into", "update", "delete from".
#[tokio::test]
async fn each_write_spells_its_own_refusal() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW ro AS SELECT DISTINCT a FROM base_tbl"
    ))
    .await;
    let mut session = engine.connect();
    let detail = "Views containing DISTINCT are not automatically updatable.";
    let cases = [
        (
            "INSERT INTO ro VALUES (1)",
            "cannot insert into view \"ro\"",
            INSERT_HINT,
        ),
        (
            "UPDATE ro SET a = 1",
            "cannot update view \"ro\"",
            UPDATE_HINT,
        ),
        (
            "DELETE FROM ro",
            "cannot delete from view \"ro\"",
            DELETE_HINT,
        ),
    ];
    for (sql, message, hint) in cases {
        assert!(
            failure(&mut session, sql).await
                == Failure::new("55000", message, Some(detail), Some(hint)),
            "{sql}"
        );
    }
}

/// A view over a view is updatable only as far as the innermost one is, and the
/// refusal names *that* view — the one a user has to fix, which is not the one
/// they wrote.
#[tokio::test]
async fn a_refusal_names_the_innermost_view_at_fault() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE}
         CREATE VIEW inner_ro AS SELECT DISTINCT a FROM base_tbl;
         CREATE VIEW outer_v AS SELECT * FROM inner_ro"
    ))
    .await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "DELETE FROM outer_v").await
            == Failure::new(
                "55000",
                "cannot delete from view \"inner_ro\"",
                Some("Views containing DISTINCT are not automatically updatable."),
                Some(DELETE_HINT),
            )
    );
}

/// Assignment is judged per column: a view mixing plain references with
/// computed expressions is updatable, and only the computed ones refuse.
#[tokio::test]
async fn a_computed_column_refuses_assignment_without_making_the_view_read_only() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW v AS SELECT a, upper(b), c FROM base_tbl"
    ))
    .await;
    let mut session = engine.connect();
    let cases = [
        (
            "INSERT INTO v VALUES (4, 'X')",
            "cannot insert into column \"upper\" of view \"v\"",
        ),
        (
            "UPDATE v SET upper = 'X' WHERE a = 1",
            "cannot update column \"upper\" of view \"v\"",
        ),
    ];
    for (sql, message) in cases {
        assert!(
            failure(&mut session, sql).await
                == Failure::new(
                    "0A000",
                    message,
                    Some(
                        "View columns that are not columns of their base relation are not \
                         updatable."
                    ),
                    None,
                ),
            "{sql}"
        );
    }
    // The updatable columns still write, and a column the view omits takes the
    // base relation's default rather than NULL.
    run(&mut session, "INSERT INTO v (a) VALUES (4)").await;
    assert!(
        query(&mut session, "SELECT a, b, c FROM base_tbl WHERE a = 4").await
            == rows(&["4,D,NULL"])
    );
}

/// The whole rewrite, one statement at a time, through a view that renames its
/// columns and hides one.
#[tokio::test]
async fn a_write_through_a_renaming_view_reaches_the_base_relation() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW v AS SELECT b AS bb, a AS aa FROM base_tbl WHERE a > 1"
    ))
    .await;
    let mut session = engine.connect();
    run(&mut session, "INSERT INTO v (aa, bb) VALUES (4, 'r4')").await;
    run(&mut session, "UPDATE v SET bb = 'R4' WHERE aa = 4").await;
    assert!(
        query(&mut session, "SELECT a, b, c FROM base_tbl ORDER BY a").await
            == rows(&["1,r1,10", "2,r2,20", "3,r3,30", "4,R4,NULL"])
    );
    // The view's own qualification restricts what a write can reach: `a = 1` is
    // outside it, so neither statement below touches that row.
    run(&mut session, "UPDATE v SET bb = 'q'").await;
    run(&mut session, "DELETE FROM v WHERE aa < 3").await;
    assert!(
        query(&mut session, "SELECT a, b FROM base_tbl ORDER BY a").await
            == rows(&["1,r1", "3,q", "4,q"])
    );
    // A column the view does not project is not in scope, even though the
    // relation underneath has it.
    assert!(
        failure(&mut session, "DELETE FROM v WHERE c = 10")
            .await
            .code
            == "42703"
    );
    assert!(
        failure(&mut session, "UPDATE v SET bb = 'z' WHERE c = 10")
            .await
            .code
            == "42703"
    );
}

/// `RETURNING` reports the view's rowtype, under the view's column names —
/// including through `*`, which must not expand to the relation underneath.
#[tokio::test]
async fn returning_keeps_the_views_columns_and_names() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW v AS SELECT b AS bb, a AS aa FROM base_tbl"
    ))
    .await;
    let mut session = engine.connect();
    let cases = [
        (
            "INSERT INTO v (aa, bb) VALUES (4, 'r4') RETURNING *",
            vec!["bb", "aa"],
            rows(&["r4,4"]),
        ),
        (
            "UPDATE v SET bb = 'R4' WHERE aa = 4 RETURNING aa, bb",
            vec!["aa", "bb"],
            rows(&["4,R4"]),
        ),
        (
            "DELETE FROM v WHERE aa = 4 RETURNING *",
            vec!["bb", "aa"],
            rows(&["R4,4"]),
        ),
    ];
    for (sql, names, _) in &cases {
        assert!(headers(&mut session, sql).await == *names, "{sql}");
    }
    // Re-run for the values: the three statements above left the row in place
    // only until the DELETE, so the sequence has to start over.
    run(&mut session, "DELETE FROM v WHERE aa = 4").await;
    for (sql, _, expected) in &cases {
        assert!(query(&mut session, sql).await == *expected, "{sql}");
    }
}

/// Two of a view's columns may select the same base column, and naming both is
/// an ambiguous assignment rather than a last-one-wins.
#[tokio::test]
async fn assigning_one_base_column_twice_through_a_view_is_refused() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE} CREATE VIEW v AS SELECT a, b, a AS aa FROM base_tbl"
    ))
    .await;
    let mut session = engine.connect();
    for sql in [
        "INSERT INTO v VALUES (9, 'r9', 9)",
        "UPDATE v SET a = 9, aa = -9",
    ] {
        let failed = failure(&mut session, sql).await;
        assert!(
            failed.message == "multiple assignments to same column \"a\"",
            "{sql}"
        );
    }
    run(&mut session, "UPDATE v SET aa = -9 WHERE a = 1").await;
    assert!(
        query(&mut session, "SELECT a FROM base_tbl ORDER BY a").await == rows(&["-9", "2", "3"])
    );
}

/// A chain of views composes into one rewrite, column map and qualification.
#[tokio::test]
async fn updatability_composes_through_nested_views() {
    let (engine, _kv) = engine_with(&format!(
        "{BASE}
         CREATE VIEW n1 AS SELECT a AS x, b AS y FROM base_tbl WHERE a > 0;
         CREATE VIEW n2 AS SELECT x AS xx, y AS yy FROM n1 WHERE x < 100;
         CREATE VIEW n3 AS SELECT xx, yy, upper(yy) AS uy FROM n2"
    ))
    .await;
    let mut session = engine.connect();
    run(
        &mut session,
        "INSERT INTO n3 (xx, yy) VALUES (20, 'twenty')",
    )
    .await;
    run(&mut session, "UPDATE n3 SET yy = 'TWENTY' WHERE xx = 20").await;
    assert!(
        query(&mut session, "SELECT a, b FROM base_tbl WHERE a = 20").await == rows(&["20,TWENTY"])
    );
    // The computed column at the top is still not assignable.
    assert!(
        failure(&mut session, "UPDATE n3 SET uy = 'X' WHERE xx = 20")
            .await
            .message
            == "cannot update column \"uy\" of view \"n3\""
    );
    run(&mut session, "DELETE FROM n3 WHERE xx = 20").await;
    assert!(
        query(&mut session, "SELECT count(*) FROM base_tbl WHERE a = 20").await == rows(&["0"])
    );
}

/// Whose check option applies, and which view a violation names.
///
/// A view enforces its own qualification when it carries any check option, and
/// every level below a `CASCADED` one enforces its own whether or not it
/// declares one — including a level below a `LOCAL` view that itself sits under
/// a cascade. A violation names the innermost view that rejected the row.
#[tokio::test]
async fn a_check_option_is_enforced_down_to_the_first_view_without_a_cascade() {
    let (engine, _kv) = engine_with(
        "CREATE TABLE t (n int);
         CREATE VIEW plain1  AS SELECT * FROM t WHERE n <> 1;
         CREATE VIEW local2  AS SELECT * FROM plain1 WHERE n <> 2 WITH LOCAL CHECK OPTION;
         CREATE VIEW casc3   AS SELECT * FROM local2 WHERE n <> 3 WITH CASCADED CHECK OPTION;
         CREATE VIEW under1  AS SELECT * FROM local2 WHERE n <> 4;
         CREATE VIEW local5  AS SELECT * FROM under1 WHERE n <> 5 WITH LOCAL CHECK OPTION",
    )
    .await;
    let mut session = engine.connect();
    // Through `casc3`: its own qual, `local2`'s (it has an option), and
    // `plain1`'s (the cascade reaches through `local2`).
    let cascaded: [(i32, Option<&str>); 4] = [
        (1, Some("plain1")),
        (2, Some("local2")),
        (3, Some("casc3")),
        (9, None),
    ];
    for (value, rejected) in cascaded {
        let sql = format!("INSERT INTO casc3 VALUES ({value})");
        match rejected {
            Some(view) => assert!(
                failure(&mut session, &sql).await
                    == Failure::new(
                        "44000",
                        &format!("new row violates check option for view \"{view}\""),
                        Some(&format!("Failing row contains ({value}).")),
                        None,
                    ),
                "{sql}"
            ),
            None => {
                run(&mut session, &sql).await;
            }
        }
    }
    // Through `local5`: its own qual and `local2`'s, but not `under1`'s (no
    // option) and not `plain1`'s (no cascade anywhere above it).
    let local: [(i32, Option<&str>); 4] = [
        (1, None),
        (2, Some("local2")),
        (4, None),
        (5, Some("local5")),
    ];
    for (value, rejected) in local {
        let sql = format!("INSERT INTO local5 VALUES ({value})");
        match rejected {
            Some(view) => assert!(
                failure(&mut session, &sql).await.message
                    == format!("new row violates check option for view \"{view}\""),
                "{sql}"
            ),
            None => {
                run(&mut session, &sql).await;
            }
        }
    }
    assert!(query(&mut session, "SELECT n FROM t ORDER BY n").await == rows(&["1", "4", "9"]));
}

/// A check option judges the row that reaches storage, so `UPDATE` is checked
/// against the row it leaves behind and the `DETAIL` shows the *base*
/// relation's columns, not the view's.
#[tokio::test]
async fn a_check_option_judges_the_stored_row() {
    let (engine, _kv) = engine_with(
        "CREATE TABLE t (a int, b int DEFAULT 10);
         CREATE VIEW v AS SELECT a AS only_a FROM t WHERE a < b WITH LOCAL CHECK OPTION",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "INSERT INTO v VALUES (1)").await;
    assert!(
        failure(&mut session, "INSERT INTO v VALUES (30)").await
            == Failure::new(
                "44000",
                "new row violates check option for view \"v\"",
                Some("Failing row contains (30, 10)."),
                None,
            )
    );
    assert!(
        failure(&mut session, "UPDATE v SET only_a = 30").await
            == Failure::new(
                "44000",
                "new row violates check option for view \"v\"",
                Some("Failing row contains (30, 10)."),
                None,
            )
    );
    assert!(query(&mut session, "SELECT a, b FROM t").await == rows(&["1,10"]));
}

/// A check option is a promise about writes, so it may only be made by a view
/// writes can be rewritten through — and the refusal names the clause at fault.
#[tokio::test]
async fn a_check_option_on_a_read_only_body_is_refused_where_it_is_written() {
    let (engine, _kv) = engine_with(BASE).await;
    let mut session = engine.connect();
    let cases = [
        (
            "CREATE VIEW bad AS SELECT DISTINCT a FROM base_tbl WITH CHECK OPTION",
            "Views containing DISTINCT are not automatically updatable.",
        ),
        (
            "CREATE VIEW bad AS SELECT count(*) FROM base_tbl WITH LOCAL CHECK OPTION",
            "Views that return aggregate functions are not automatically updatable.",
        ),
    ];
    for (sql, hint) in cases {
        assert!(
            failure(&mut session, sql).await
                == Failure::new(
                    "0A000",
                    "WITH CHECK OPTION is supported only on automatically updatable views",
                    None,
                    Some(hint),
                ),
            "{sql}"
        );
    }
}

/// `ALTER VIEW … SET (check_option = …)` turns the enforcement on, and `RESET`
/// turns it back off — which is also what stops a parent's cascade at that
/// level.
#[tokio::test]
async fn altering_the_check_option_reloption_changes_what_is_enforced() {
    const REPORTED: &str =
        "SELECT check_option FROM information_schema.views WHERE table_name = 'v'";
    let (engine, _kv) = engine_with(
        "CREATE TABLE t (a int);
         CREATE VIEW v AS SELECT * FROM t WHERE a > 0",
    )
    .await;
    let mut session = engine.connect();
    run(&mut session, "INSERT INTO v VALUES (-1)").await;
    assert!(query(&mut session, REPORTED).await == rows(&["NONE"]));
    run(&mut session, "ALTER VIEW v SET (check_option = cascaded)").await;
    assert!(query(&mut session, REPORTED).await == rows(&["CASCADED"]));
    assert!(
        failure(&mut session, "INSERT INTO v VALUES (-2)")
            .await
            .code
            == "44000",
        "the option must take effect"
    );
    run(&mut session, "ALTER VIEW v SET (check_option = local)").await;
    assert!(query(&mut session, REPORTED).await == rows(&["LOCAL"]));
    run(&mut session, "ALTER VIEW v RESET (check_option)").await;
    assert!(query(&mut session, REPORTED).await == rows(&["NONE"]));
    run(&mut session, "INSERT INTO v VALUES (-3)").await;
    assert!(query(&mut session, "SELECT a FROM t ORDER BY a").await == rows(&["-3", "-1"]));
}

/// An `INSTEAD OF` trigger takes the write back from the rewrite, and a check
/// option above such a view still applies to the row the trigger is handed —
/// but a cascade does not reach past it, because the rewrite stops there.
#[tokio::test]
async fn an_instead_of_trigger_ends_the_rewrite_and_the_cascade() {
    let (engine, _kv) = engine_with(
        "CREATE TABLE t (a int, b int);
         CREATE VIEW inner_v AS SELECT a FROM t WHERE a < b;
         CREATE FUNCTION f() RETURNS trigger AS $$
           BEGIN INSERT INTO t VALUES (NEW.a, 10); RETURN NEW; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER trig INSTEAD OF INSERT ON inner_v
           FOR EACH ROW EXECUTE PROCEDURE f();
         CREATE VIEW outer_v AS SELECT * FROM inner_v WHERE a > 0
           WITH CASCADED CHECK OPTION",
    )
    .await;
    let mut session = engine.connect();
    assert!(
        failure(&mut session, "INSERT INTO outer_v VALUES (-5)").await
            == Failure::new(
                "44000",
                "new row violates check option for view \"outer_v\"",
                Some("Failing row contains (-5)."),
                None,
            )
    );
    // 100 fails `inner_v`'s own qual (100 < 10 is false), and the cascade does
    // not reach it: the trigger, not the rewrite, performs the write.
    run(&mut session, "INSERT INTO outer_v VALUES (100)").await;
    assert!(query(&mut session, "SELECT a, b FROM t ORDER BY a").await == rows(&["100,10"]));
}

/// `ON CONFLICT` rewrites too: the arbiter's columns and the `DO UPDATE`
/// targets are assignments, and `excluded` presents the view's rowtype — so a
/// view column the relation underneath has no column for is still readable
/// there.
#[tokio::test]
async fn on_conflict_rewrites_through_a_view() {
    let (engine, _kv) = engine_with(
        "CREATE TABLE t (a text UNIQUE, b int);
         INSERT INTO t VALUES ('k', 0);
         CREATE VIEW v AS SELECT b, b + 1 AS c, a, 2 AS two FROM t",
    )
    .await;
    let mut session = engine.connect();
    run(
        &mut session,
        "INSERT INTO v (a, b) VALUES ('k', 1) ON CONFLICT (a) DO UPDATE SET b = v.b + 5",
    )
    .await;
    assert!(query(&mut session, "SELECT a, b FROM t").await == rows(&["k,5"]));
    run(
        &mut session,
        "INSERT INTO v (a, b) VALUES ('k', 7) ON CONFLICT (a) DO UPDATE SET b = excluded.two",
    )
    .await;
    assert!(query(&mut session, "SELECT a, b FROM t").await == rows(&["k,2"]));
    run(
        &mut session,
        "INSERT INTO v (a, b) VALUES ('k', 9) ON CONFLICT (a) DO UPDATE SET b = excluded.b \
         WHERE excluded.c > 0",
    )
    .await;
    assert!(query(&mut session, "SELECT a, b FROM t").await == rows(&["k,9"]));
    assert!(
        failure(
            &mut session,
            "INSERT INTO v (a, b) VALUES ('k', 1) ON CONFLICT (a) DO UPDATE SET two = 3"
        )
        .await
        .message
            == "cannot insert into column \"two\" of view \"v\""
    );
}

/// A write through a view is decided under the view's owner, exactly as a read
/// through one is — so a role granted the view but not the relation underneath
/// may write, and one granted neither may not.
#[tokio::test]
async fn a_write_through_a_view_runs_as_the_views_owner() {
    let (engine, _kv) = engine_with(
        "CREATE USER owner_role;
         CREATE USER writer_role;
         GRANT CREATE ON SCHEMA public TO owner_role;",
    )
    .await;
    let mut session = engine.connect();
    run(
        &mut session,
        "SET SESSION AUTHORIZATION owner_role;
         CREATE TABLE owned (a int, b text);
         INSERT INTO owned VALUES (1, 'x');
         CREATE VIEW v AS SELECT * FROM owned WHERE a > 0;
         GRANT SELECT, INSERT, UPDATE, DELETE ON v TO writer_role;
         RESET SESSION AUTHORIZATION;
         SET SESSION AUTHORIZATION writer_role;",
    )
    .await;
    assert!(
        failure(&mut session, "INSERT INTO owned VALUES (2, 'y')")
            .await
            .code
            == "42501"
    );
    run(&mut session, "INSERT INTO v VALUES (2, 'y')").await;
    run(&mut session, "UPDATE v SET b = 'Y' WHERE a = 2").await;
    run(&mut session, "DELETE FROM v WHERE a = 2").await;
    assert!(query(&mut session, "SELECT a, b FROM v").await == rows(&["1,x"]));
    // Revoking the write on the *view* is enough to stop the write, even though
    // the owner still has every right on the relation underneath.
    run(
        &mut session,
        "RESET SESSION AUTHORIZATION;
         SET SESSION AUTHORIZATION owner_role;
         REVOKE INSERT ON v FROM writer_role;
         RESET SESSION AUTHORIZATION;
         SET SESSION AUTHORIZATION writer_role;",
    )
    .await;
    assert!(
        failure(&mut session, "INSERT INTO v VALUES (3, 'z')").await
            == Failure::new("42501", "permission denied for view v", None, None)
    );
}

/// A row-level policy on the relation underneath applies to a write rewritten
/// through a view, and is decided under the *view's owner* — the identity the
/// view's body already reads under.
///
/// Which is why the policy has to be `FORCE`d to bite at all here: the view's
/// owner is the relation's owner, and an unforced policy does not apply to it.
/// Both halves are asserted, because the bypass is the more surprising one and
/// it is the one `PostgreSQL` actually does.
#[tokio::test]
async fn row_security_on_the_base_relation_applies_as_the_views_owner() {
    let setup = "CREATE USER rls_owner;
         CREATE USER rls_writer;
         GRANT CREATE ON SCHEMA public TO rls_owner;
         SET SESSION AUTHORIZATION rls_owner;
         CREATE TABLE t (a int, b text);
         INSERT INTO t VALUES (1, 'x'), (50, 'big');
         CREATE VIEW v AS SELECT * FROM t;
         GRANT SELECT, INSERT, UPDATE, DELETE ON v TO rls_writer;
         ALTER TABLE t ENABLE ROW LEVEL SECURITY;
         CREATE POLICY p ON t USING (a < 10) WITH CHECK (a < 10);
         RESET SESSION AUTHORIZATION;
         SET SESSION AUTHORIZATION rls_writer;";

    // Unforced: the view's owner owns the relation, so the policy does not
    // apply to a write rewritten through the view — even though the writer is
    // a different role entirely.
    let (engine, _kv) = engine_with(setup).await;
    let mut session = engine.connect();
    run(&mut session, "UPDATE v SET b = 'seen' WHERE a = 50").await;
    run(&mut session, "INSERT INTO v VALUES (99, 'no')").await;
    assert!(
        query(&mut session, "SELECT a, b FROM v ORDER BY a").await
            == rows(&["1,x", "50,seen", "99,no"])
    );

    // Forced: the same policy now judges the write, hiding the row the `USING`
    // qual excludes and rejecting the row the `WITH CHECK` qual excludes.
    let forced = setup.replace(
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY;",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY;
         ALTER TABLE t FORCE ROW LEVEL SECURITY;",
    );
    let (engine, _kv) = engine_with(&forced).await;
    let mut session = engine.connect();
    assert!(query(&mut session, "SELECT a, b FROM v ORDER BY a").await == rows(&["1,x"]));
    run(&mut session, "UPDATE v SET b = 'seen' WHERE a = 50").await;
    assert!(query(&mut session, "SELECT a, b FROM v ORDER BY a").await == rows(&["1,x"]));
    assert!(
        failure(&mut session, "UPDATE v SET a = 99 WHERE a = 1")
            .await
            .code
            == "42501"
    );
    assert!(
        failure(&mut session, "INSERT INTO v VALUES (99, 'no')")
            .await
            .code
            == "42501"
    );
    run(&mut session, "INSERT INTO v VALUES (2, 'ok')").await;
    assert!(query(&mut session, "SELECT a, b FROM v ORDER BY a").await == rows(&["1,x", "2,ok"]));
}
