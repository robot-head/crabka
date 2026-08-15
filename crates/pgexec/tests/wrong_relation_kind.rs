//! A relation of the wrong *kind* is refused as one, not reported as missing.
//!
//! Views, sequences and indexes live under catalog keys that
//! `crabka_pgcatalog::get_table` does not read, so a statement that only tried
//! that lookup answered 42P01 — "relation does not exist" — for a name whose
//! relation is sitting right there. `PostgreSQL` answers 42809 and says what
//! the relation actually is, and for a whole family of refusals the message
//! carries the reason in a `DETAIL` naming the kind.
//!
//! Every expectation here was measured against `PostgreSQL` 18.4; the wordings
//! are not derived from one another, because `PostgreSQL`'s are not either.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Engine, Session},
    error::PgError,
};

/// The whole reportable shape of a statement's outcome, so a case compares one
/// value rather than three fields. `DETAIL` is the half that carries the
/// reason, and it is the half an engine most easily drops.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    Failed {
        code: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

impl From<PgError> for Outcome {
    fn from(error: PgError) -> Self {
        let diagnostics = error.diagnostics.unwrap_or_default();
        Self::Failed {
            code: error.code,
            message: error.message,
            detail: diagnostics.detail,
            hint: diagnostics.hint,
        }
    }
}

/// 42809 with no `DETAIL` and no `HINT`.
fn refused(message: &str) -> Outcome {
    Outcome::Failed {
        code: "42809".to_string(),
        message: message.to_string(),
        detail: None,
        hint: None,
    }
}

/// 42809 carrying `PostgreSQL`'s `errdetail_relkind_not_supported`, whose
/// wording is the plural of the kind that was found.
fn refused_for(message: &str, plural_kind: &str) -> Outcome {
    Outcome::Failed {
        code: "42809".to_string(),
        message: message.to_string(),
        detail: Some(format!(
            "This operation is not supported for {plural_kind}."
        )),
        hint: None,
    }
}

async fn outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.simple_query(sql).await {
        Ok(_) => Outcome::Ok,
        Err(error) => error.into(),
    }
}

/// A schema holding one relation of each kind, so a case names the kind it
/// means. The schema is not `public`, which is what proves the refusals spell
/// the relation the way the statement wrote it rather than schema-qualified.
async fn fixture() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE SCHEMA sh",
        "SET search_path = sh, public",
        "CREATE TABLE st (i int)",
        "INSERT INTO st VALUES (1)",
        "CREATE VIEW sv AS SELECT * FROM st",
        "CREATE SEQUENCE ss",
        "CREATE INDEX st_idx ON st (i)",
        "CREATE MATERIALIZED VIEW smv AS SELECT * FROM st",
    ] {
        session
            .simple_query(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e:?}"));
    }
    (engine, session)
}

async fn check(cases: &[(&str, Outcome)]) {
    let (_engine, mut session) = fixture().await;
    for (sql, expected) in cases {
        let actual = outcome(&mut session, sql).await;
        assert!(actual == *expected, "{sql}");
    }
}

/// `TRUNCATE` refuses every kind it cannot empty with the one wording:
/// `truncate_check_rel` names the kind it wanted, not the one it found, and
/// emits no `HINT` because there is no command it could suggest instead.
#[tokio::test]
async fn truncate_names_the_kind_it_wanted_for_every_relation_it_cannot_empty() {
    check(&[
        ("TRUNCATE sv", refused("\"sv\" is not a table")),
        ("TRUNCATE ss", refused("\"ss\" is not a table")),
        ("TRUNCATE st_idx", refused("\"st_idx\" is not a table")),
        ("TRUNCATE smv", refused("\"smv\" is not a table")),
        // The kind it can empty still works, and the list form refuses as a
        // whole rather than emptying the good half first.
        ("TRUNCATE st, sv", refused("\"sv\" is not a table")),
        ("SELECT count(*) FROM st", Outcome::Ok),
    ])
    .await;
    let (_engine, mut session) = fixture().await;
    // TRUNCATE is all-or-nothing: the refused list left the table untouched.
    let rows = session.simple_query("TRUNCATE st, sv").await;
    assert!(rows.is_err());
}

/// `CLUSTER` rejects a relation with no heap while the name is still being
/// opened. A materialized view has one, gets past that, and is refused later
/// for having no clustered index — a different SQLSTATE entirely.
#[tokio::test]
async fn cluster_separates_a_relation_with_no_heap_from_one_with_no_clustered_index() {
    check(&[
        (
            "CLUSTER sv",
            refused("\"sv\" is not a table or materialized view"),
        ),
        (
            "CLUSTER ss",
            refused("\"ss\" is not a table or materialized view"),
        ),
        (
            "CLUSTER st_idx",
            refused("\"st_idx\" is not a table or materialized view"),
        ),
        (
            "CLUSTER smv",
            Outcome::Failed {
                code: "42704".to_string(),
                message: "there is no previously clustered index for table \"smv\"".to_string(),
                detail: None,
                hint: None,
            },
        ),
    ])
    .await;
}

/// `ALTER TABLE` refuses per *subcommand*, not per relation: which kinds a
/// subcommand accepts is `PostgreSQL`'s `ATSimplePermissions` mask, and it has
/// no pattern to it. So a materialized view takes `RENAME COLUMN` and refuses
/// `ADD COLUMN`, and the refusal always says which kind stopped it.
#[tokio::test]
async fn alter_table_refuses_per_subcommand_and_names_the_kind() {
    check(&[
        (
            "ALTER TABLE sv ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"sv\"",
                "views",
            ),
        ),
        (
            "ALTER TABLE ss ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"ss\"",
                "sequences",
            ),
        ),
        (
            "ALTER TABLE st_idx ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"st_idx\"",
                "indexes",
            ),
        ),
        (
            "ALTER TABLE smv ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"smv\"",
                "materialized views",
            ),
        ),
        // IF EXISTS does not suppress it: the relation exists, it is only of
        // the wrong kind.
        (
            "ALTER TABLE IF EXISTS ss ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"ss\"",
                "sequences",
            ),
        ),
        // A subcommand the kind does accept is not refused, which is what makes
        // this a per-subcommand rule rather than a blanket one.
        ("ALTER TABLE smv RENAME COLUMN i TO j", Outcome::Ok),
        // The first refused subcommand in written order is the one reported.
        (
            "ALTER TABLE smv RENAME COLUMN j TO k, ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"smv\"",
                "materialized views",
            ),
        ),
    ])
    .await;
}

/// The subcommand names in these refusals are `PostgreSQL`'s, which are not
/// always the words the statement was written with: the row-security four drop
/// the `LEVEL`, and the trigger form spells the selector back.
///
/// These were only ever reachable for a view before, and were wrong there.
#[tokio::test]
async fn a_refused_subcommand_is_named_the_way_postgresql_names_it() {
    check(&[
        (
            "ALTER TABLE sv ENABLE ROW LEVEL SECURITY",
            refused_for(
                "ALTER action ENABLE ROW SECURITY cannot be performed on relation \"sv\"",
                "views",
            ),
        ),
        (
            "ALTER TABLE sv NO FORCE ROW LEVEL SECURITY",
            refused_for(
                "ALTER action NO FORCE ROW SECURITY cannot be performed on relation \"sv\"",
                "views",
            ),
        ),
        (
            "ALTER TABLE ss ENABLE TRIGGER ALL",
            refused_for(
                "ALTER action ENABLE TRIGGER ALL cannot be performed on relation \"ss\"",
                "sequences",
            ),
        ),
        (
            "ALTER TABLE ss DISABLE TRIGGER tg",
            refused_for(
                "ALTER action DISABLE TRIGGER cannot be performed on relation \"ss\"",
                "sequences",
            ),
        ),
        (
            "ALTER TABLE ss ENABLE REPLICA TRIGGER tg",
            refused_for(
                "ALTER action ENABLE REPLICA TRIGGER cannot be performed on relation \"ss\"",
                "sequences",
            ),
        ),
        // Renaming is worded by the rename path rather than the subcommand
        // table, so it names no action at all.
        (
            "ALTER TABLE ss RENAME COLUMN last_value TO v",
            refused_for("cannot rename columns of relation \"ss\"", "sequences"),
        ),
    ])
    .await;
}

/// `LOCK TABLE` takes a view — `PostgreSQL` locks it and, recursively, what it
/// reads — and refuses the other three, materialized views included. So this
/// cannot be phrased as "everything without a heap".
#[tokio::test]
async fn lock_table_accepts_a_view_and_refuses_the_kinds_that_have_no_lock_to_take() {
    // A refusal aborts the block it was issued in, so each case gets its own.
    check(&[
        ("BEGIN", Outcome::Ok),
        ("LOCK TABLE st", Outcome::Ok),
        ("LOCK TABLE sv", Outcome::Ok),
        ("ROLLBACK", Outcome::Ok),
        ("BEGIN", Outcome::Ok),
        (
            "LOCK TABLE ss",
            refused_for("cannot lock relation \"ss\"", "sequences"),
        ),
        ("ROLLBACK", Outcome::Ok),
        ("BEGIN", Outcome::Ok),
        (
            "LOCK TABLE st_idx",
            refused_for("cannot lock relation \"st_idx\"", "indexes"),
        ),
        ("ROLLBACK", Outcome::Ok),
        ("BEGIN", Outcome::Ok),
        (
            "LOCK TABLE smv",
            refused_for("cannot lock relation \"smv\"", "materialized views"),
        ),
        ("ROLLBACK", Outcome::Ok),
        // The whole list is resolved before any of it is locked, so a wrong
        // kind anywhere in it refuses before the good names are taken.
        ("BEGIN", Outcome::Ok),
        (
            "LOCK TABLE st, ss",
            refused_for("cannot lock relation \"ss\"", "sequences"),
        ),
        ("ROLLBACK", Outcome::Ok),
    ])
    .await;
}

/// A name that belongs to nothing at all is still the caller's own 42P01. The
/// wrong-kind refusals must not swallow it.
#[tokio::test]
async fn a_name_no_relation_holds_is_still_reported_as_missing() {
    // Written schema-qualified, which is the spelling PostgreSQL echoes back
    // for a qualified name — so the case turns on the SQLSTATE, not on how the
    // missing name is spelled.
    let missing = |name: &str| Outcome::Failed {
        code: "42P01".to_string(),
        message: format!("relation \"{name}\" does not exist"),
        detail: None,
        hint: None,
    };
    check(&[
        ("TRUNCATE sh.nosuch", missing("sh.nosuch")),
        ("CLUSTER sh.nosuch", missing("sh.nosuch")),
        ("SELECT * FROM sh.nosuch", missing("sh.nosuch")),
        ("BEGIN", Outcome::Ok),
        ("LOCK TABLE sh.nosuch", missing("sh.nosuch")),
        ("ROLLBACK", Outcome::Ok),
    ])
    .await;
}

/// Table privileges are the privileges a view, a sequence and a materialized
/// view all hold, so granting on them is ordinary. An index holds none, and
/// `PostgreSQL` says what the relation *is* rather than what it is not.
#[tokio::test]
async fn granting_on_an_index_is_refused_and_the_other_kinds_are_not() {
    check(&[
        ("GRANT SELECT ON sv TO PUBLIC", Outcome::Ok),
        ("GRANT SELECT ON ss TO PUBLIC", Outcome::Ok),
        ("GRANT SELECT ON smv TO PUBLIC", Outcome::Ok),
        (
            "GRANT SELECT ON st_idx TO PUBLIC",
            refused("\"st_idx\" is an index"),
        ),
        (
            "GRANT SELECT ON TABLE st_idx TO PUBLIC",
            refused("\"st_idx\" is an index"),
        ),
        (
            "REVOKE SELECT ON st_idx FROM PUBLIC",
            refused("\"st_idx\" is an index"),
        ),
        // A list is refused as a whole rather than granting the good half.
        (
            "GRANT SELECT ON st, st_idx TO PUBLIC",
            refused("\"st_idx\" is an index"),
        ),
    ])
    .await;
}

/// An index cannot be opened as a relation at all, so every statement that
/// would read or write one reports the same thing — `relation_open` refuses it
/// before any statement-specific rule runs.
#[tokio::test]
async fn an_index_cannot_be_opened_by_any_statement_that_reads_or_writes() {
    let cannot_open = refused_for("cannot open relation \"st_idx\"", "indexes");
    check(&[
        ("SELECT * FROM st_idx", cannot_open.clone()),
        ("SELECT i FROM st_idx WHERE i = 1", cannot_open.clone()),
        ("INSERT INTO st_idx VALUES (9)", cannot_open.clone()),
        ("UPDATE st_idx SET i = 1", cannot_open.clone()),
        ("DELETE FROM st_idx", cannot_open.clone()),
        (
            "SELECT * FROM st JOIN st_idx USING (i)",
            cannot_open.clone(),
        ),
    ])
    .await;
    // Copy-out enters through its own entry point rather than `simple_query`.
    let (_engine, mut session) = fixture().await;
    assert!(copy_out_outcome(&mut session, "COPY st_idx TO STDOUT").await == cannot_open);
}

/// The copy-out entry point, which `simple_query` cannot reach: a copy to
/// STDOUT needs the wire layer's `CopyOut` frames.
async fn copy_out_outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.begin_copy_out(sql).await {
        Ok(_) => Outcome::Ok,
        Err(error) => error.into(),
    }
}

/// The kinds a write statement cannot change, each in its own wording: a
/// sequence is named as a sequence, and a view that is auto-updatable is
/// written through rather than refused.
#[tokio::test]
async fn a_write_names_the_kind_it_cannot_change() {
    check(&[
        (
            "INSERT INTO ss VALUES (9)",
            refused("cannot change sequence \"ss\""),
        ),
        ("DELETE FROM ss", refused("cannot change sequence \"ss\"")),
        (
            "INSERT INTO smv VALUES (9)",
            refused("cannot change materialized view \"smv\""),
        ),
        // The auto-updatable view is unaffected: it is still written through.
        ("INSERT INTO sv VALUES (9)", Outcome::Ok),
    ])
    .await;
}

/// `COPY` has its own wording per direction and per kind, and for a view it
/// points at what would make the copy work.
#[tokio::test]
async fn copy_names_the_kind_and_the_direction() {
    check(&[
        (
            "COPY sv FROM STDIN",
            Outcome::Failed {
                code: "42809".to_string(),
                message: "cannot copy to view \"sv\"".to_string(),
                detail: None,
                hint: Some(
                    "To enable copying to a view, provide an INSTEAD OF INSERT trigger."
                        .to_string(),
                ),
            },
        ),
        (
            "COPY ss FROM STDIN",
            refused("cannot copy to sequence \"ss\""),
        ),
        (
            "COPY st_idx FROM STDIN",
            refused_for("cannot open relation \"st_idx\"", "indexes"),
        ),
    ])
    .await;
    let (_engine, mut session) = fixture().await;
    assert!(
        copy_out_outcome(&mut session, "COPY sv TO STDOUT").await
            == Outcome::Failed {
                code: "42809".to_string(),
                message: "cannot copy from view \"sv\"".to_string(),
                detail: None,
                hint: Some("Try the COPY (SELECT ...) TO variant.".to_string()),
            }
    );
    assert!(
        copy_out_outcome(&mut session, "COPY ss TO STDOUT").await
            == refused("cannot copy from sequence \"ss\"")
    );
}

/// A wrong-kind refusal names the relation the way the statement wrote it, not
/// schema-qualified — these relations all live in `sh`, and none of the
/// messages says so.
#[tokio::test]
async fn a_refusal_spells_the_relation_bare_even_outside_public() {
    check(&[
        (
            "COMMENT ON TABLE sv IS 'c'",
            refused("\"sv\" is not a table"),
        ),
        (
            "COMMENT ON TABLE ss IS 'c'",
            refused("\"ss\" is not a table"),
        ),
        ("COMMENT ON VIEW st IS 'c'", refused("\"st\" is not a view")),
        // Written schema-qualified, and still spelled the way it was written.
        ("TRUNCATE sh.sv", refused("\"sv\" is not a table")),
    ])
    .await;
}
