//! `GENERATED ALWAYS AS (…) VIRTUAL`, `PostgreSQL` 18's new — and now default —
//! kind of generated column.
//!
//! The whole feature is one difference from `STORED`: a virtual column's value
//! is **never written down**. The row keeps a NULL placeholder at the column's
//! position and every reader recomputes the value from the expression the
//! catalog holds at that moment.
//!
//! Two tests below pin that difference in a way an implementation that quietly
//! computed the value on write could not pass:
//! [`an_expression_that_overflows_is_an_error_on_read_not_on_write`] inserts a
//! row whose expression cannot be evaluated at all, and
//! [`changing_the_expression_changes_what_rows_written_earlier_report`] rewords
//! the expression without touching a row.
//!
//! The other consequence of "computed when it is read" is *who* reads it, which
//! upstream answers by expanding the expression only where the statement
//! references the column.
//! [`a_row_whose_expression_raises_can_still_be_deleted_and_truncated`] is why
//! that matters rather than being an optimization,
//! [`a_reader_outside_the_statement_still_sees_the_computed_value`] is the side
//! that makes narrowing sound, and
//! [`a_trigger_sees_null_for_a_virtual_generated_column`] is the one reader
//! upstream keeps the value away from. Everything else here is the surface that
//! has to agree with them.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
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

/// Every row of the first result, each rendered as a comma-joined string so one
/// expectation is one literal.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &run(session, sql).await[0] {
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

// ── The value is not stored ──────────────────────────────────────────────────

/// The expression is evaluated on every read, so one that cannot be evaluated
/// at all still lets the row be written and then fails whoever reads it.
///
/// A `STORED` column has to evaluate it to have anything to write, which is why
/// the same insert is rejected there. No implementation that computed a virtual
/// column on write could tell these two apart.
///
/// The error is per-column, not per-relation: only a statement that can observe
/// the column evaluates it. Every expectation here was measured against
/// `PostgreSQL` 18.4, including the two that look inconsistent side by side —
/// `SELECT *` raises and `SELECT a` over the same row does not.
#[tokio::test]
async fn an_expression_that_overflows_is_an_error_on_read_not_on_write() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE ovf_v (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "CREATE TABLE ovf_s (a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
    ])
    .await;

    run(&mut session, "INSERT INTO ovf_v (a) VALUES (2000000000)").await;
    for sql in ["SELECT * FROM ovf_v", "SELECT b FROM ovf_v"] {
        assert!(
            error(&mut session, sql).await
                == ("22003".to_string(), "integer out of range".to_string()),
            "{sql}"
        );
    }
    // The same row, read by statements that never name the column.
    assert!(query(&mut session, "SELECT a FROM ovf_v").await == vec!["2000000000"]);
    assert!(query(&mut session, "SELECT count(*) FROM ovf_v").await == vec!["1"]);

    // Rewording the expression makes it evaluable again, and the row that was
    // written all along reports its value — proof it was stored regardless.
    run(
        &mut session,
        "ALTER TABLE ovf_v ALTER COLUMN b SET EXPRESSION AS (a / 2)",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM ovf_v").await == vec!["2000000000,1000000000"]);

    assert!(
        error(&mut session, "INSERT INTO ovf_s (a) VALUES (2000000000)").await
            == ("22003".to_string(), "integer out of range".to_string())
    );
}

/// A row whose expression raises has to stay removable, or the relation holding
/// it can never be emptied.
///
/// This is the whole reason the read is narrowed. `DELETE … WHERE a = …` and
/// `TRUNCATE` reach nothing but the plain column, so neither evaluates the
/// generation expression — `PostgreSQL` 18.4 runs both, and an engine that
/// materialized every virtual column on every scan answered 22003 to both and
/// left the row in place for good.
#[tokio::test]
async fn a_row_whose_expression_raises_can_still_be_deleted_and_truncated() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE poison (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO poison (a) VALUES (1), (2000000000)",
    ])
    .await;

    run(&mut session, "DELETE FROM poison WHERE a = 2000000000").await;
    assert!(query(&mut session, "SELECT * FROM poison").await == vec!["1,2"]);

    // And with the row back, the unfiltered form each of them desugars to.
    run(&mut session, "INSERT INTO poison (a) VALUES (2000000000)").await;
    run(&mut session, "TRUNCATE poison").await;
    assert!(query(&mut session, "SELECT count(*) FROM poison").await == vec!["0"]);

    run(&mut session, "INSERT INTO poison (a) VALUES (2000000000)").await;
    run(&mut session, "UPDATE poison SET a = 3 WHERE a = 2000000000").await;
    assert!(query(&mut session, "SELECT * FROM poison").await == vec!["3,6"]);

    run(&mut session, "DELETE FROM poison").await;
}

/// The reader a statement's own text gives no sign of.
///
/// Narrowing the read to the columns the statement spells is only sound while
/// every other reader of the row is asked for, and a row-security `USING` qual
/// is written in the catalog rather than in the statement.
///
/// A second — a foreign key carried by the generated column itself — is widened
/// for as well, and is deliberately not asserted here. `PostgreSQL` 18.4
/// refuses the constraint outright ("foreign key constraints on virtual
/// generated columns are not supported"); this engine accepts the DDL and then
/// enforces nothing, because the key it reads out of storage is the NULL
/// placeholder and a NULL key satisfies every foreign key. Making the widening
/// observable would mean fixing that first.
#[tokio::test]
async fn a_reader_outside_the_statement_still_sees_the_computed_value() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE policied (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO policied (a) VALUES (1), (5)",
        "ALTER TABLE policied ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY visible ON policied USING (b < 5)",
        "CREATE ROLE reader",
        "GRANT SELECT, DELETE ON policied TO reader",
    ])
    .await;

    // The policy names a column the statement does not, and still hides the row.
    run(&mut session, "SET ROLE reader").await;
    assert!(query(&mut session, "SELECT a FROM policied").await == vec!["1"]);
    run(&mut session, "DELETE FROM policied WHERE a = 5").await;
    run(&mut session, "RESET ROLE").await;
    assert!(query(&mut session, "SELECT a FROM policied ORDER BY a").await == vec!["1", "5"]);
}

/// A trigger never sees a virtual generated column at all.
///
/// `PostgreSQL`: "it is not allowed to access generated columns in `BEFORE`
/// triggers" — the value is conceptually settled after they have run — and its
/// `AFTER` images carry the same NULL. A value the trigger *assigns* to one is
/// dropped as well (`check_modified_virtual_generated`), so it reaches neither
/// the next trigger nor the row that gets written.
///
/// The `WHERE` on the delete names the column on purpose: it makes the write
/// path materialize the value into the very row `OLD` is taken from, so the
/// NULL below can only come from the blanking and not from the narrowing.
#[tokio::test]
async fn a_trigger_sees_null_for_a_virtual_generated_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE triggered (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "CREATE TABLE seen (tag text, b int)",
        "CREATE FUNCTION record_images() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN
             IF TG_OP = 'DELETE' THEN
               INSERT INTO seen VALUES ('old', OLD.b);
               RETURN OLD;
             END IF;
             INSERT INTO seen VALUES ('new', NEW.b);
             NEW.b := 300;
             RETURN NEW;
           END $$",
        "CREATE TRIGGER images BEFORE INSERT OR DELETE ON triggered FOR EACH ROW
           EXECUTE FUNCTION record_images()",
        "INSERT INTO triggered (a) VALUES (7)",
        "DELETE FROM triggered WHERE b = 14",
    ])
    .await;

    assert!(
        query(&mut session, "SELECT tag, b FROM seen ORDER BY tag").await
            == vec!["new,NULL", "old,NULL"]
    );
    // The delete found its row, so the qual did see the value the trigger did
    // not — and the 300 the trigger assigned was never stored.
    assert!(query(&mut session, "SELECT count(*) FROM triggered").await == vec!["0"]);
}

/// `SET EXPRESSION` rewords a virtual column without rewriting a row, so rows
/// written long before it report the new value.
#[tokio::test]
async fn changing_the_expression_changes_what_rows_written_earlier_report() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE setexpr (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO setexpr (a) VALUES (3), (4)",
    ])
    .await;

    assert!(query(&mut session, "SELECT * FROM setexpr ORDER BY a").await == vec!["3,6", "4,8"]);
    run(
        &mut session,
        "ALTER TABLE setexpr ALTER COLUMN b SET EXPRESSION AS (a * 10)",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM setexpr ORDER BY a").await == vec!["3,30", "4,40"]);

    // A `STORED` column reaches the same values, but only because the statement
    // rewrote every row to get there.
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE setexpr_s (a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
        "INSERT INTO setexpr_s (a) VALUES (3), (4)",
        "ALTER TABLE setexpr_s ALTER COLUMN b SET EXPRESSION AS (a * 10)",
    ])
    .await;
    assert!(
        query(&mut session, "SELECT * FROM setexpr_s ORDER BY a").await == vec!["3,30", "4,40"]
    );
}

// ── Reading ──────────────────────────────────────────────────────────────────

/// Every reader agrees, because they all reach the rows through the one scan
/// that fills the column in.
#[tokio::test]
async fn a_virtual_column_reads_back_through_every_query_shape() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE reads (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO reads (a) VALUES (3), (4)",
        "CREATE VIEW reads_v AS SELECT * FROM reads",
        "CREATE TABLE other (x int, y int)",
        "INSERT INTO other VALUES (30, 3), (40, 4)",
    ])
    .await;

    for (sql, expected) in [
        ("SELECT * FROM reads ORDER BY a", vec!["3,6", "4,8"]),
        ("SELECT a, b FROM reads ORDER BY a", vec!["3,6", "4,8"]),
        ("SELECT b FROM reads ORDER BY a", vec!["6", "8"]),
        ("SELECT b * 2 FROM reads ORDER BY a", vec!["12", "16"]),
        ("SELECT a FROM reads WHERE b = 8", vec!["4"]),
        ("SELECT a FROM reads WHERE b > 6 ORDER BY a", vec!["4"]),
        ("SELECT * FROM reads_v ORDER BY a", vec!["3,6", "4,8"]),
        (
            "WITH c AS (SELECT * FROM reads) SELECT * FROM c ORDER BY a",
            vec!["3,6", "4,8"],
        ),
        (
            "SELECT other.x, reads.b FROM other, reads WHERE other.y = reads.a ORDER BY 1",
            vec!["30,6", "40,8"],
        ),
        ("SELECT max(b) FROM reads", vec!["8"]),
        ("SELECT a FROM reads ORDER BY b DESC", vec!["4", "3"]),
    ] {
        assert!(query(&mut session, sql).await == expected, "{sql}");
    }
}

/// A write hands back the value the next reader would see, not the placeholder
/// it actually stored.
#[tokio::test]
async fn returning_shows_the_computed_value_for_both_images() {
    let (_engine, mut session) =
        engine_with(&["CREATE TABLE ret (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)"]).await;

    for (sql, expected) in [
        ("INSERT INTO ret (a) VALUES (3) RETURNING *", vec!["3,6"]),
        ("UPDATE ret SET a = 5 RETURNING *", vec!["5,10"]),
        ("DELETE FROM ret RETURNING *", vec!["5,10"]),
    ] {
        assert!(query(&mut session, sql).await == expected, "{sql}");
    }
}

/// `UPDATE`/`DELETE` choose their rows through a different scan than `SELECT`
/// does, so the qual has to see the value there too.
#[tokio::test]
async fn update_and_delete_can_qualify_on_a_virtual_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE quals (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO quals (a) VALUES (1), (2), (3)",
    ])
    .await;

    run(&mut session, "UPDATE quals SET a = 9 WHERE b = 4").await;
    assert!(
        query(&mut session, "SELECT * FROM quals ORDER BY a").await == vec!["1,2", "3,6", "9,18"]
    );
    run(&mut session, "DELETE FROM quals WHERE b = 6").await;
    assert!(query(&mut session, "SELECT * FROM quals ORDER BY a").await == vec!["1,2", "9,18"]);
}

// ── COPY ─────────────────────────────────────────────────────────────────────

/// `COPY` never carries a generated column, in either direction.
///
/// Upstream's `CopyGetAttnums` leaves one out of the default column list and
/// refuses one written in an explicit list, and both directions resolve their
/// list through it. The kind does not matter, so `STORED` is checked beside
/// `VIRTUAL`.
///
/// The load side's refusal has to arrive from `begin_copy_in`, before the mode
/// is announced: psql reads the rest of its script as COPY data once
/// `CopyInResponse` has gone out, so a refusal that arrives later eats every
/// statement up to the next `\.`.
#[tokio::test]
async fn copy_leaves_a_generated_column_out_and_refuses_one_written_down() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE cpv (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "CREATE TABLE cps (a int, b int GENERATED ALWAYS AS (a * 3) STORED)",
        "INSERT INTO cpv (a) VALUES (1), (2)",
        "INSERT INTO cps (a) VALUES (1), (2)",
    ])
    .await;

    for relation in ["cpv", "cps"] {
        let stream = session
            .begin_copy_out(&format!("COPY {relation} TO stdout"))
            .await
            .unwrap_or_else(|error| panic!("{relation}: {error:?}"))
            .unwrap_or_else(|| panic!("{relation} should be a copy-out"));
        let payload = stream.rows.concat();
        assert!(
            String::from_utf8(payload).expect("utf8") == "1\n2\n",
            "{relation}"
        );

        let refusal = (
            "42P10".to_string(),
            "column \"b\" is a generated column".to_string(),
        );
        let out = session
            .begin_copy_out(&format!("COPY {relation} (a, b) TO stdout"))
            .await
            .err()
            .unwrap_or_else(|| panic!("{relation} copy-out should have been refused"));
        assert!((out.code.clone(), out.message) == refusal, "{relation} TO");

        let into = session
            .begin_copy_in(&format!("COPY {relation} (a, b) FROM stdin"))
            .await
            .err()
            .unwrap_or_else(|| panic!("{relation} copy-in should have been refused"));
        assert!(
            (into.code.clone(), into.message) == refusal,
            "{relation} IN"
        );
    }

    // The default list loads the plain column alone, and the generated one is
    // computed rather than demanded.
    for relation in ["cpv", "cps"] {
        let sql = format!("COPY {relation} FROM stdin");
        session
            .begin_copy_in(&sql)
            .await
            .unwrap_or_else(|error| panic!("{relation}: {error:?}"));
        session
            .copy_in(&sql, 0, vec![bytes::Bytes::from_static(b"3\n4\n")])
            .await
            .unwrap_or_else(|error| panic!("{relation}: {error:?}"));
    }
    assert!(
        query(&mut session, "SELECT * FROM cpv ORDER BY a").await
            == vec!["1,2", "2,4", "3,6", "4,8"]
    );
    assert!(
        query(&mut session, "SELECT * FROM cps ORDER BY a").await
            == vec!["1,3", "2,6", "3,9", "4,12"]
    );
}

// ── Constraints ──────────────────────────────────────────────────────────────

/// `NOT NULL` and `CHECK` are the two things that make a write evaluate a
/// virtual column, and both are checked against the value, not the placeholder.
#[tokio::test]
async fn constraints_over_a_virtual_column_see_its_value() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE cnn (a int, b int GENERATED ALWAYS AS (nullif(a, 0)) VIRTUAL NOT NULL)",
        "CREATE TABLE cck (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL CHECK (b < 50))",
        "CREATE TABLE clate (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO clate (a) VALUES (10), (30)",
    ])
    .await;

    run(&mut session, "INSERT INTO cnn (a) VALUES (1)").await;
    assert!(
        error(&mut session, "INSERT INTO cnn (a) VALUES (0)").await
            == (
                "23502".to_string(),
                "null value in column \"b\" of relation \"cnn\" violates not-null constraint"
                    .to_string()
            )
    );

    run(&mut session, "INSERT INTO cck (a) VALUES (10)").await;
    assert!(
        error(&mut session, "INSERT INTO cck (a) VALUES (30)")
            .await
            .0
            == "23514"
    );

    // A constraint added later is validated against rows that store nothing for
    // the column it names.
    assert!(
        error(&mut session, "ALTER TABLE clate ADD CHECK (b < 50)")
            .await
            .0
            == "23514"
    );
    run(&mut session, "ALTER TABLE clate ADD CHECK (b < 100)").await;
}

/// A column added to a populated relation reports its value at once, without a
/// rewrite — and a `NOT NULL` on it is still checked against every row.
#[tokio::test]
async fn add_column_gives_existing_rows_the_value_without_rewriting_them() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE addcol (a int)",
        "INSERT INTO addcol VALUES (3), (4)",
        "ALTER TABLE addcol ADD COLUMN b int GENERATED ALWAYS AS (a * 2) VIRTUAL",
    ])
    .await;

    assert!(query(&mut session, "SELECT * FROM addcol ORDER BY a").await == vec!["3,6", "4,8"]);
    assert!(
        error(
            &mut session,
            "ALTER TABLE addcol ADD COLUMN c int NOT NULL GENERATED ALWAYS AS (nullif(a, 3)) \
             VIRTUAL",
        )
        .await
        .0 == "23502"
    );
    run(
        &mut session,
        "ALTER TABLE addcol ADD COLUMN c int NOT NULL GENERATED ALWAYS AS (nullif(a, 9)) VIRTUAL",
    )
    .await;
    assert!(query(&mut session, "SELECT * FROM addcol ORDER BY a").await == vec!["3,6,3", "4,8,4"]);
}

// ── Catalog ──────────────────────────────────────────────────────────────────

/// `pg_attribute.attgenerated` distinguishes the three cases.
#[tokio::test]
async fn attgenerated_reports_the_kind_of_each_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE attgen (plain int, stored int GENERATED ALWAYS AS (plain * 2) STORED, \
         virt int GENERATED ALWAYS AS (plain * 3) VIRTUAL, \
         defaulted int GENERATED ALWAYS AS (plain * 4))",
    ])
    .await;

    assert!(
        query(
            &mut session,
            "SELECT attname, attgenerated FROM pg_attribute WHERE attrelid = 'attgen'::regclass \
             ORDER BY attnum",
        )
        .await
            == vec!["plain,", "stored,s", "virt,v", "defaulted,v"]
    );
}

// ── The restrictions ─────────────────────────────────────────────────────────

/// `PostgreSQL` 18 refuses these at DDL time. The messages are upstream's, and
/// the kind of the column is irrelevant to all but the index cases.
#[tokio::test]
async fn a_generation_expression_is_refused_where_postgres_refuses_it() {
    let (_engine, mut session) = engine_with(&["CREATE TABLE plain_rel (a int, b int)"]).await;

    for (sql, sqlstate, message) in [
        (
            "CREATE TABLE g (a int, b int DEFAULT 5 GENERATED ALWAYS AS (a * 2) VIRTUAL)",
            "42601",
            "both default and generation expression specified for column \"b\" of table \"g\"",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS IDENTITY GENERATED ALWAYS AS (a * 2) \
             VIRTUAL)",
            "42601",
            "both identity and generation expression specified for column \"b\" of table \"g\"",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL GENERATED ALWAYS AS \
             (a * 3) VIRTUAL)",
            "42601",
            "multiple generation clauses specified for column \"b\" of table \"g\"",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED BY DEFAULT AS (a * 2) VIRTUAL)",
            "42601",
            "for a generated column, GENERATED ALWAYS must be specified",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS (b * 2) VIRTUAL)",
            "42P17",
            "cannot use generated column \"b\" in column generation expression",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL, c int GENERATED \
             ALWAYS AS (b * 3) VIRTUAL)",
            "42P17",
            "cannot use generated column \"b\" in column generation expression",
        ),
        (
            "CREATE TABLE g (a int, b bool GENERATED ALWAYS AS (xmin <> 37) VIRTUAL)",
            "42P17",
            "cannot use system column \"xmin\" in column generation expression",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS (c * 2) VIRTUAL)",
            "42703",
            "column \"c\" does not exist",
        ),
        (
            "CREATE TABLE g (a int, b double precision GENERATED ALWAYS AS (random()) VIRTUAL)",
            "42P17",
            "generation expression is not immutable",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS (avg(a)) VIRTUAL)",
            "42803",
            "aggregate functions are not allowed in column generation expressions",
        ),
        (
            "CREATE TABLE g (a int, b int GENERATED ALWAYS AS ((SELECT a)) VIRTUAL)",
            "0A000",
            "cannot use subquery in column generation expression",
        ),
    ] {
        assert!(
            error(&mut session, sql).await == (sqlstate.to_string(), message.to_string()),
            "{sql}"
        );
    }

    // The immutability test is accurate: a plain concatenation is fine.
    run(
        &mut session,
        "CREATE TABLE g_ok (a int, b text GENERATED ALWAYS AS (a || ' sec') VIRTUAL)",
    )
    .await;
}

/// A virtual column cannot be keyed on: the value an index entry would hold is
/// produced by an expression the catalog can change without any row being
/// written.
#[tokio::test]
async fn a_virtual_column_cannot_be_indexed() {
    let (_engine, mut session) =
        engine_with(&["CREATE TABLE ix (a int, b int GENERATED ALWAYS AS (a / 2) VIRTUAL)"]).await;

    for (sql, message) in [
        (
            "CREATE TABLE ixa (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a / 2) VIRTUAL UNIQUE)",
            "unique constraints on virtual generated columns are not supported",
        ),
        (
            "CREATE TABLE ixb (a int, b int GENERATED ALWAYS AS (a / 2) VIRTUAL, PRIMARY KEY (a, b))",
            "primary keys on virtual generated columns are not supported",
        ),
        (
            "CREATE INDEX ix_b ON ix (b)",
            "indexes on virtual generated columns are not supported",
        ),
        (
            "ALTER TABLE ix ADD UNIQUE (b)",
            "unique constraints on virtual generated columns are not supported",
        ),
        (
            "ALTER TABLE ix ADD PRIMARY KEY (b)",
            "primary keys on virtual generated columns are not supported",
        ),
    ] {
        assert!(
            error(&mut session, sql).await == ("0A000".to_string(), message.to_string()),
            "{sql}"
        );
    }

    // The same index over the plain column is unaffected.
    run(&mut session, "CREATE INDEX ix_a ON ix (a)").await;
}

/// A statement may only ever name `DEFAULT` for a generated column, whichever
/// kind it is.
#[tokio::test]
async fn a_write_may_not_supply_a_value_for_a_generated_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE wr_v (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "CREATE TABLE wr_s (a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
        "INSERT INTO wr_v (a) VALUES (1)",
        "INSERT INTO wr_s (a) VALUES (1)",
    ])
    .await;

    for (sql, message) in [
        (
            "INSERT INTO wr_v VALUES (2, 33)",
            "cannot insert a non-DEFAULT value into column \"b\"",
        ),
        (
            "INSERT INTO wr_s VALUES (2, 33)",
            "cannot insert a non-DEFAULT value into column \"b\"",
        ),
        (
            "INSERT INTO wr_v VALUES (2, DEFAULT), (3, 44)",
            "cannot insert a non-DEFAULT value into column \"b\"",
        ),
        (
            "UPDATE wr_v SET b = 11",
            "column \"b\" can only be updated to DEFAULT",
        ),
        (
            "UPDATE wr_s SET b = 11",
            "column \"b\" can only be updated to DEFAULT",
        ),
    ] {
        assert!(
            error(&mut session, sql).await == ("428C9".to_string(), message.to_string()),
            "{sql}"
        );
    }

    // `DEFAULT` is accepted and means "compute it".
    run(&mut session, "INSERT INTO wr_v VALUES (2, DEFAULT)").await;
    run(&mut session, "UPDATE wr_v SET b = DEFAULT WHERE a = 2").await;
    assert!(query(&mut session, "SELECT * FROM wr_v ORDER BY a").await == vec!["1,2", "2,4"]);
}

/// `DROP EXPRESSION` turns a stored generated column into an ordinary one. A
/// virtual column has no values to leave behind, so `PostgreSQL` 18 refuses.
#[tokio::test]
async fn drop_expression_demotes_a_stored_column_and_refuses_a_virtual_one() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE de_s (a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
        "CREATE TABLE de_v (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "INSERT INTO de_s (a) VALUES (3)",
        "INSERT INTO de_v (a) VALUES (3)",
    ])
    .await;

    run(
        &mut session,
        "ALTER TABLE de_s ALTER COLUMN b DROP EXPRESSION",
    )
    .await;
    // The column keeps the value it last computed and is now writable.
    assert!(query(&mut session, "SELECT * FROM de_s").await == vec!["3,6"]);
    run(&mut session, "INSERT INTO de_s VALUES (4, 99)").await;
    assert!(query(&mut session, "SELECT * FROM de_s ORDER BY a").await == vec!["3,6", "4,99"]);
    assert!(
        query(
            &mut session,
            "SELECT attgenerated FROM pg_attribute WHERE attrelid = 'de_s'::regclass AND attname \
             = 'b'",
        )
        .await
            == vec![""]
    );

    assert!(
        error(
            &mut session,
            "ALTER TABLE de_v ALTER COLUMN b DROP EXPRESSION"
        )
        .await
            == (
                "0A000".to_string(),
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
                    .to_string()
            )
    );
}

/// `SET EXPRESSION` and `DROP EXPRESSION` both need a column that has one, and
/// `IF EXISTS` is about the expression rather than the column.
#[tokio::test]
async fn set_and_drop_expression_require_a_generated_column() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE se (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL, CHECK (a > 0))",
        "CREATE TABLE se_plain (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)",
    ])
    .await;

    for sql in [
        "ALTER TABLE se_plain ALTER COLUMN a SET EXPRESSION AS (a * 3)",
        "ALTER TABLE se_plain ALTER COLUMN a DROP EXPRESSION",
    ] {
        assert!(
            error(&mut session, sql).await
                == (
                    "42611".to_string(),
                    "column \"a\" of relation \"se_plain\" is not a generated column".to_string()
                ),
            "{sql}"
        );
    }
    // `IF EXISTS` makes the same subcommand a no-op.
    run(
        &mut session,
        "ALTER TABLE se_plain ALTER COLUMN a DROP EXPRESSION IF EXISTS",
    )
    .await;

    // A relation carrying CHECK constraints cannot have a virtual column's
    // expression reworded, because the constraints would have to be revalidated
    // against values that are stored nowhere.
    assert!(
        error(
            &mut session,
            "ALTER TABLE se ALTER COLUMN b SET EXPRESSION AS (a * 3)"
        )
        .await
            == (
                "0A000".to_string(),
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in \
                 tables with check constraints"
                    .to_string()
            )
    );
}

// ── STORED, unchanged ────────────────────────────────────────────────────────

/// The kind that already worked still does, and `VIRTUAL` is what a clause that
/// names neither means.
#[tokio::test]
async fn stored_keeps_its_behaviour_and_an_unqualified_clause_is_virtual() {
    let (_engine, mut session) = engine_with(&[
        "CREATE TABLE kinds (a int, s int GENERATED ALWAYS AS (a * 2) STORED, \
         v int GENERATED ALWAYS AS (a * 3) VIRTUAL, d int GENERATED ALWAYS AS (a * 4))",
        "INSERT INTO kinds (a) VALUES (2)",
    ])
    .await;

    assert!(query(&mut session, "SELECT * FROM kinds").await == vec!["2,4,6,8"]);
    run(&mut session, "UPDATE kinds SET a = 5").await;
    assert!(query(&mut session, "SELECT * FROM kinds").await == vec!["5,10,15,20"]);
    assert!(
        query(
            &mut session,
            "SELECT a FROM kinds WHERE s = 10 AND v = 15 AND d = 20"
        )
        .await
            == vec!["5"]
    );
}
