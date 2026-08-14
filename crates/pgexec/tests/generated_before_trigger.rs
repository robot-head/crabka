//! When a generated column settles, and what a `BEFORE ROW` trigger may still
//! do to the row before it does.
//!
//! `PostgreSQL` states the rule in `doc/src/sgml/ddl.sgml`: "Generated columns
//! are, conceptually, updated after `BEFORE` triggers have run". Everything
//! here follows from that one sentence.
//!
//! A trigger returns a *replacement* row, and the replacement is what gets
//! written. So a `STORED` column computed before the trigger holds a value the
//! trigger's row never produced, and a constraint checked before the trigger
//! judges a row nobody stores. Both are silent: the first writes a wrong number
//! under a column whose declared meaning is its expression, and the second
//! refuses a row the trigger was about to repair.
//!
//! The trigger's own view is the other half of the rule. Upstream does not let
//! a `BEFORE` trigger read a generated column at all, so `NEW.b` is NULL there
//! whichever kind the column is — the value has not settled yet, and there is
//! nothing honest to report.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of the first result, each rendered as a comma-joined string so one
/// expectation is one literal.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    let results = session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    match &results[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("{sql} should return rows, got {other:?}"),
    }
}

/// The SQLSTATE and message of a statement that must fail.
async fn error(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should fail"));
    (error.code, error.message)
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in setup {
        run(&mut session, sql).await;
    }
    (engine, session)
}

/// A trigger that rewrites the plain column and also assigns to the generated
/// one. The assignment to the generated column is the part that must be
/// discarded; the rewrite of the plain column is the part the generated column
/// must be recomputed from.
const LAUNDER: &str = "CREATE FUNCTION launder() RETURNS trigger LANGUAGE plpgsql AS $$ \
                       BEGIN NEW.a := NEW.a * 10; NEW.b := 300; RETURN NEW; END $$";

// ── The value the trigger leaves behind is the one that settles ──────────────

/// `PostgreSQL` 18.4's own `generated_stored` case, "check that modifications of
/// generated columns in triggers do not get propagated".
///
/// Two `BEFORE INSERT` triggers fire in name order. The first sees the row as
/// written, the second multiplies `a` by ten and assigns 300 to `b`. Upstream
/// stores `10 | 20`: the assignment to `b` is dropped and the expression is
/// evaluated once, over the row the last trigger returned.
///
/// An engine that settles first stores `10 | 300` — a `STORED` generated column
/// holding a number its own expression cannot produce from any row.
#[tokio::test]
async fn a_stored_column_is_computed_from_the_row_the_last_trigger_returned() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE gtest26 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) STORED)",
        LAUNDER,
        "CREATE TRIGGER t01 BEFORE INSERT ON gtest26 FOR EACH ROW EXECUTE FUNCTION launder()",
    ])
    .await;

    run(&mut session, "INSERT INTO gtest26 (a) VALUES (1)").await;
    assert!(query(&mut session, "SELECT * FROM gtest26").await == vec!["10,20"]);
}

/// The same rule at every write path that fires a row trigger.
///
/// Each case ends with the statement assigning `1` to `a`, the trigger turning
/// that into `10`, and the generated column having to report `20`. One
/// expectation, six ways in — because the settle is one function and a path
/// that reached storage around it would be the whole bug back.
#[tokio::test]
async fn every_write_path_settles_after_its_before_trigger() {
    let seeded = "INSERT INTO trg (id, a) VALUES (1, 7)";
    for (what, seed, write) in [
        ("INSERT", None, "INSERT INTO trg (id, a) VALUES (1, 1)"),
        ("UPDATE", Some(seeded), "UPDATE trg SET a = 1 WHERE id = 1"),
        (
            "MERGE … INSERT",
            None,
            "MERGE INTO trg USING (VALUES (1, 1)) v(id, a) ON trg.id = v.id \
             WHEN NOT MATCHED THEN INSERT (id, a) VALUES (v.id, v.a)",
        ),
        (
            "MERGE … UPDATE",
            Some(seeded),
            "MERGE INTO trg USING (VALUES (1, 1)) v(id, a) ON trg.id = v.id \
             WHEN MATCHED THEN UPDATE SET a = v.a",
        ),
        (
            "ON CONFLICT DO UPDATE",
            Some(seeded),
            "INSERT INTO trg (id, a) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET a = 1",
        ),
        (
            "UPDATE … FROM",
            Some(seeded),
            "UPDATE trg SET a = v.a FROM (VALUES (1, 1)) v(id, a) WHERE trg.id = v.id",
        ),
    ] {
        let (_engine, mut session) = engine_with(&[
            "CREATE TABLE trg (id int PRIMARY KEY, a int, \
             b int GENERATED ALWAYS AS (a * 2) STORED)",
            LAUNDER,
            "CREATE TRIGGER t_i BEFORE INSERT ON trg FOR EACH ROW EXECUTE FUNCTION launder()",
            "CREATE TRIGGER t_u BEFORE UPDATE ON trg FOR EACH ROW EXECUTE FUNCTION launder()",
        ])
        .await;
        if let Some(seed) = seed {
            run(&mut session, seed).await;
        }
        run(&mut session, write).await;
        assert!(
            query(&mut session, "SELECT * FROM trg").await == vec!["1,10,20"],
            "{what}"
        );
    }
}

/// `COPY … FROM` settles each row where every other write path does, and the
/// copy's own `CONTEXT` line still names the line a constraint rejected.
///
/// The settle used to happen inside the row builder, which held the copy line
/// and could name it. It now happens a layer up, so the line has to be carried
/// there — a `COPY` whose failures stopped saying which line failed would be a
/// worse answer than the one this change fixes.
#[tokio::test]
async fn a_copied_row_settles_and_still_reports_its_line() {
    let (_engine, mut session) = engine_with(&["CREATE TABLE cp (id int PRIMARY KEY, a int, \
         b int GENERATED ALWAYS AS (a * 2) STORED CHECK (b < 100))"])
    .await;

    let sql = "COPY cp (id, a) FROM STDIN";
    session
        .begin_copy_in(sql)
        .await
        .expect("copy-in should be accepted");
    session
        .copy_in(
            sql,
            0,
            vec![bytes::Bytes::from_static(b"1\t1\n2\t2\n\\.\n")],
        )
        .await
        .expect("copy should succeed");
    assert!(query(&mut session, "SELECT * FROM cp ORDER BY id").await == vec!["1,1,2", "2,2,4"]);

    session
        .begin_copy_in(sql)
        .await
        .expect("copy-in should be accepted");
    let error = session
        .copy_in(
            sql,
            0,
            vec![bytes::Bytes::from_static(b"3\t3\n4\t60\n\\.\n")],
        )
        .await
        .expect_err("the second line violates the CHECK");
    assert!(error.code == "23514");
    assert!(
        error
            .diagnostics
            .and_then(|fields| fields.context)
            .as_deref()
            == Some("COPY cp, line 2: \"4\t60\"")
    );
}

// ── The trigger's own view ───────────────────────────────────────────────────

/// A `BEFORE` trigger reads NULL out of a generated column of either kind,
/// because the value has not settled yet.
///
/// The trigger writes what it saw into a second relation, so the assertion is
/// about the image the trigger was handed rather than about a notice.
#[tokio::test]
async fn a_before_trigger_reads_null_from_a_generated_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE seen (s int, v int)",
        "CREATE TABLE pair (a int, s int GENERATED ALWAYS AS (a * 2) STORED, \
         v int GENERATED ALWAYS AS (a * 3) VIRTUAL)",
        "CREATE FUNCTION peek() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO seen VALUES (NEW.s, NEW.v); RETURN NEW; END $$",
        "CREATE TRIGGER t_i BEFORE INSERT ON pair FOR EACH ROW EXECUTE FUNCTION peek()",
    ])
    .await;

    run(&mut session, "INSERT INTO pair (a) VALUES (4)").await;
    assert!(query(&mut session, "SELECT * FROM seen").await == vec!["NULL,NULL"]);
    assert!(query(&mut session, "SELECT * FROM pair").await == vec!["4,8,12"]);
}

/// An `UPDATE` is the case that makes the rule cost something to obey: its
/// proposed row is built from the stored row, so a `STORED` generated column
/// arrives carrying the value it held *before* the statement.
///
/// `PostgreSQL` 18.4 prints `new = (11,)` for the update below, not `(11,20)`.
/// The trigger has to see nothing there, because the number the row will end up
/// with has not been computed yet and the number it used to have is about to be
/// replaced. `OLD` is the opposite: it is a stored row, and it reports the
/// value it really holds.
#[tokio::test]
async fn an_update_hands_its_before_trigger_no_old_value_in_a_generated_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE seen (tag text, s int)",
        "CREATE TABLE upd (a int, s int GENERATED ALWAYS AS (a * 2) STORED)",
        "CREATE FUNCTION peek() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
         INSERT INTO seen VALUES (\'old\', OLD.s); \
         INSERT INTO seen VALUES (\'new\', NEW.s); \
         RETURN NEW; END $$",
        "INSERT INTO upd (a) VALUES (10)",
        "CREATE TRIGGER t_u BEFORE UPDATE ON upd FOR EACH ROW EXECUTE FUNCTION peek()",
    ])
    .await;

    run(&mut session, "UPDATE upd SET a = 11").await;
    assert!(
        query(&mut session, "SELECT tag, s FROM seen ORDER BY tag").await
            == vec!["new,NULL", "old,20"]
    );
    assert!(query(&mut session, "SELECT * FROM upd").await == vec!["11,22"]);
}

// ── The constraints judge the same row ───────────────────────────────────────

/// `NOT NULL` and `CHECK` are enforced against the row the trigger leaves, so a
/// trigger can repair a row the statement proposed badly — and can spoil one it
/// proposed well.
///
/// Both directions in one relation, because a fix that moved only the generated
/// columns and left the constraints where they were would pass the first half
/// and fail the second.
#[tokio::test]
async fn the_constraints_judge_the_row_the_trigger_leaves() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE guard (a int NOT NULL, b int GENERATED ALWAYS AS (a * 2) STORED, \
         CHECK (b < 100))",
        "CREATE FUNCTION mend() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
         IF NEW.a IS NULL THEN NEW.a := 4; END IF; \
         IF NEW.a = 60 THEN NEW.a := 5; END IF; \
         IF NEW.a = 1 THEN NEW.a := 70; END IF; \
         RETURN NEW; END $$",
        "CREATE TRIGGER t_i BEFORE INSERT ON guard FOR EACH ROW EXECUTE FUNCTION mend()",
    ])
    .await;

    // Repaired: the proposed row violates NOT NULL, the trigger's does not.
    run(&mut session, "INSERT INTO guard (a) VALUES (NULL)").await;
    // Repaired: 60 would make `b` 120 and fail the CHECK; the trigger's 5 does
    // not.
    run(&mut session, "INSERT INTO guard (a) VALUES (60)").await;
    assert!(query(&mut session, "SELECT * FROM guard ORDER BY a").await == vec!["4,8", "5,10"]);

    // Spoiled: the proposed row passes both, the trigger's row fails the CHECK
    // through the generated column it never touched.
    let (code, _) = error(&mut session, "INSERT INTO guard (a) VALUES (1)").await;
    assert!(code == "23514");
}
