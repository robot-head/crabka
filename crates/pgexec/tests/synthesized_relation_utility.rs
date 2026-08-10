//! A synthesised catalog relation is present, and present as a *kind*.
//!
//! `pg_class` and `information_schema.tables` are answered from the engine
//! rather than from storage, so `crabka_pgcatalog::relation_exists` and
//! `get_table` — which read stored keys only — said they were not there. Every
//! utility statement that re-asked existence that way reported 42P01 for a
//! relation `PostgreSQL` resolves.
//!
//! Two answers are needed, not one. `PostgreSQL` synthesises these as two
//! relkinds, and the split is exactly the one `pg_class.relkind` already
//! projects: a synthesised *table* is one of the catalogs `allowSystemTableMods`
//! protects, so a statement that would rewrite its definition is 42501; a
//! synthesised *view* is an ordinary view and takes the wrong-kind refusals.
//! Measured over all 71 synthesised relations against `PostgreSQL` 18.4,
//! `relkind = 'r'` and "pinned" coincide exactly.
//!
//! Every expectation here was measured against `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Engine, Session},
    error::PgError,
};

/// The whole reportable shape of a statement's outcome, so a case compares one
/// value rather than four fields.
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

fn failed(code: &str, message: &str) -> Outcome {
    Outcome::Failed {
        code: code.to_string(),
        message: message.to_string(),
        detail: None,
        hint: None,
    }
}

/// 42809 with neither `DETAIL` nor `HINT`.
fn refused(message: &str) -> Outcome {
    failed("42809", message)
}

/// 42809 carrying `PostgreSQL`'s `errdetail_relkind_not_supported`.
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

/// 42809 carrying `DropErrorMsgWrongType`'s `HINT`, which names the command
/// that would have worked on the kind that is really there.
fn wrong_drop(message: &str, hint: &str) -> Outcome {
    Outcome::Failed {
        code: "42809".to_string(),
        message: message.to_string(),
        detail: None,
        hint: Some(hint.to_string()),
    }
}

/// `allowSystemTableMods`. A privilege refusal, not a kind one, and it names
/// the relation bare however the statement qualified it.
fn system_catalog(relation: &str) -> Outcome {
    failed(
        "42501",
        &format!("permission denied: \"{relation}\" is a system catalog"),
    )
}

async fn outcome(session: &mut SqlSession, sql: &str) -> Outcome {
    match session.simple_query(sql).await {
        Ok(_) => Outcome::Ok,
        Err(error) => error.into(),
    }
}

/// One relation of every stored kind, so a case that names no synthesised
/// relation still has one of each to aim at. The schema is not `public`, which
/// is what proves a refusal spells the relation the way the statement wrote it.
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

/// A statement that would rewrite a system catalog's definition is refused as a
/// privilege, whichever statement it is and whichever subcommand it carries —
/// `PostgreSQL` tests this in the range-var callback every `ALTER` shares,
/// before the subcommand list is looked at.
#[tokio::test]
async fn a_statement_that_would_rewrite_a_synthesized_table_is_a_privilege_refusal() {
    check(&[
        ("TRUNCATE pg_class", system_catalog("pg_class")),
        ("DROP TABLE pg_class", system_catalog("pg_class")),
        (
            "ALTER TABLE pg_class ADD COLUMN z int",
            system_catalog("pg_class"),
        ),
        (
            "ALTER TABLE pg_class DROP COLUMN relname",
            system_catalog("pg_class"),
        ),
        (
            "ALTER TABLE pg_class ALTER COLUMN relname DROP NOT NULL",
            system_catalog("pg_class"),
        ),
        (
            "ALTER TABLE pg_class RENAME TO zz",
            system_catalog("pg_class"),
        ),
        (
            "ALTER TABLE pg_class OWNER TO crab",
            system_catalog("pg_class"),
        ),
        (
            "CREATE INDEX ON pg_class (relname)",
            system_catalog("pg_class"),
        ),
        (
            "CREATE TABLE fkchild (a int REFERENCES pg_class)",
            system_catalog("pg_class"),
        ),
        // Every synthesised relation whose kind is `table` is one of them, not
        // just the four `\d` reads most.
        ("TRUNCATE pg_proc", system_catalog("pg_proc")),
        ("TRUNCATE pg_ts_config", system_catalog("pg_ts_config")),
        ("TRUNCATE pg_depend", system_catalog("pg_depend")),
        // IF EXISTS waives a missing relation, and this one is not missing.
        (
            "ALTER TABLE IF EXISTS pg_class ADD COLUMN z int",
            system_catalog("pg_class"),
        ),
        ("DROP TABLE IF EXISTS pg_class", system_catalog("pg_class")),
        // The message carries the name the statement wrote, which is bare even
        // when the statement qualified it.
        ("TRUNCATE pg_catalog.pg_class", system_catalog("pg_class")),
        (
            "ALTER TABLE pg_catalog.pg_class ADD COLUMN z int",
            system_catalog("pg_class"),
        ),
    ])
    .await;
}

/// A synthesised *view* is not pinned, so none of that applies to it: it takes
/// the ordinary wrong-kind refusals, the same ones a stored view takes.
#[tokio::test]
async fn a_synthesized_view_takes_the_wrong_kind_path_rather_than_the_privilege_one() {
    check(&[
        (
            "TRUNCATE pg_settings",
            refused("\"pg_settings\" is not a table"),
        ),
        (
            "TRUNCATE information_schema.tables",
            refused("\"tables\" is not a table"),
        ),
        (
            "CLUSTER pg_settings",
            refused("\"pg_settings\" is not a table or materialized view"),
        ),
        (
            "ALTER TABLE pg_settings ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"pg_settings\"",
                "views",
            ),
        ),
        (
            "ALTER TABLE information_schema.tables ADD COLUMN z int",
            refused_for(
                "ALTER action ADD COLUMN cannot be performed on relation \"tables\"",
                "views",
            ),
        ),
        (
            "CREATE INDEX ON pg_settings (name)",
            refused_for("cannot create index on relation \"pg_settings\"", "views"),
        ),
        (
            "CREATE TABLE fkchild (a int REFERENCES pg_settings)",
            refused("referenced relation \"pg_settings\" is not a table"),
        ),
        (
            "CREATE TABLE inh () INHERITS (pg_settings)",
            refused("inherited relation \"pg_settings\" is not a table or foreign table"),
        ),
    ])
    .await;
}

/// `DROP <kind>` names the kind that was asked for and hints the command that
/// would have worked — and it runs ahead of the privilege test, so
/// `DROP VIEW pg_class` is a kind mismatch where `DROP TABLE pg_class` is not.
#[tokio::test]
async fn drop_of_a_synthesized_relation_names_the_kind_and_hints_the_command() {
    let table_hint = "Use DROP TABLE to remove a table.";
    let view_hint = "Use DROP VIEW to remove a view.";
    check(&[
        (
            "DROP VIEW pg_class",
            wrong_drop("\"pg_class\" is not a view", table_hint),
        ),
        (
            "DROP MATERIALIZED VIEW pg_class",
            wrong_drop("\"pg_class\" is not a materialized view", table_hint),
        ),
        (
            "DROP INDEX pg_class",
            wrong_drop("\"pg_class\" is not an index", table_hint),
        ),
        (
            "DROP SEQUENCE pg_class",
            wrong_drop("\"pg_class\" is not a sequence", table_hint),
        ),
        (
            "DROP FOREIGN TABLE pg_class",
            wrong_drop("\"pg_class\" is not a foreign table", table_hint),
        ),
        (
            "DROP TABLE pg_settings",
            wrong_drop("\"pg_settings\" is not a table", view_hint),
        ),
        (
            "DROP INDEX information_schema.tables",
            wrong_drop("\"tables\" is not an index", view_hint),
        ),
    ])
    .await;
}

/// The statements that only *read* a relation, or record something beside it,
/// succeed on a synthesised one exactly as they do on a stored one — and the
/// kind still decides which spelling of `COMMENT ON` is accepted.
#[tokio::test]
async fn reading_statements_treat_a_synthesized_relation_as_present() {
    check(&[
        ("COMMENT ON TABLE pg_class IS 'c'", Outcome::Ok),
        ("COMMENT ON COLUMN pg_class.relname IS 'c'", Outcome::Ok),
        (
            "COMMENT ON VIEW pg_class IS 'c'",
            refused("\"pg_class\" is not a view"),
        ),
        ("COMMENT ON VIEW pg_settings IS 'c'", Outcome::Ok),
        (
            "COMMENT ON TABLE pg_settings IS 'c'",
            refused("\"pg_settings\" is not a table"),
        ),
        ("GRANT SELECT ON pg_proc TO PUBLIC", Outcome::Ok),
        ("REVOKE SELECT ON pg_proc FROM PUBLIC", Outcome::Ok),
        ("GRANT SELECT ON pg_settings TO PUBLIC", Outcome::Ok),
        // Existence moved to this side of the catalog seam with the
        // synthesised half, so the name that belongs to nothing still has to
        // be refused here.
        (
            "GRANT SELECT ON nosuch TO PUBLIC",
            failed("42P01", "relation \"nosuch\" does not exist"),
        ),
        (
            "REVOKE SELECT ON nosuch FROM PUBLIC",
            failed("42P01", "relation \"nosuch\" does not exist"),
        ),
        (
            "GRANT SELECT ON st_idx TO PUBLIC",
            refused("\"st_idx\" is an index"),
        ),
        ("VACUUM pg_class", Outcome::Ok),
        ("ANALYZE pg_class", Outcome::Ok),
        ("BEGIN", Outcome::Ok),
        ("LOCK TABLE pg_class", Outcome::Ok),
        ("LOCK TABLE pg_settings", Outcome::Ok),
        ("COMMIT", Outcome::Ok),
        // A column the relation does not have is still reported against the
        // relation rather than as a missing relation.
        (
            "COMMENT ON COLUMN pg_class.nosuch IS 'c'",
            failed(
                "42703",
                "column \"nosuch\" of relation \"pg_catalog.pg_class\" does not exist",
            ),
        ),
        // A name the engine synthesises nothing for keeps its 42P01.
        (
            "COMMENT ON TABLE pg_catalog.pg_nosuch IS 'c'",
            failed("42P01", "relation \"pg_catalog.pg_nosuch\" does not exist"),
        ),
    ])
    .await;
}

/// `CLUSTER` and `REFRESH MATERIALIZED VIEW` both split on whether the relation
/// has a heap, and a synthesised *table* is on the heap-bearing side of both —
/// so neither reports it as the wrong kind and neither reports it as absent.
#[tokio::test]
async fn a_synthesized_table_gets_past_the_checks_that_ask_for_a_heap() {
    check(&[
        (
            "CLUSTER pg_class",
            failed(
                "42704",
                "there is no previously clustered index for table \"pg_class\"",
            ),
        ),
        (
            "REFRESH MATERIALIZED VIEW pg_class",
            failed("0A000", "\"pg_class\" is not a materialized view"),
        ),
        // The view half takes the other branch, which is a different SQLSTATE
        // and a different wording.
        (
            "REFRESH MATERIALIZED VIEW pg_settings",
            refused("\"pg_settings\" is not a table or materialized view"),
        ),
        (
            "CLUSTER information_schema.tables",
            refused("\"tables\" is not a table or materialized view"),
        ),
    ])
    .await;
}

/// `COPY` reads a synthesised table through the `SELECT` it rewrites to, and
/// refuses a synthesised view with the wording — and the `HINT` — a stored view
/// gets.
#[tokio::test]
async fn copy_reads_a_synthesized_table_and_refuses_a_synthesized_view() {
    let from_view = |relation: &str| Outcome::Failed {
        code: "42809".to_string(),
        message: format!("cannot copy from view \"{relation}\""),
        detail: None,
        hint: Some("Try the COPY (SELECT ...) TO variant.".to_string()),
    };
    // Copy-out enters through its own entry point rather than `simple_query`,
    // because delivering the rows needs the wire layer's `CopyOut` frames.
    let (_engine, mut session) = fixture().await;
    for (sql, expected) in [
        ("COPY pg_settings TO STDOUT", from_view("pg_settings")),
        (
            "COPY information_schema.tables TO STDOUT",
            from_view("tables"),
        ),
        (
            "COPY pg_class (nosuch) TO STDOUT",
            failed(
                "42703",
                "column \"nosuch\" of relation \"pg_class\" does not exist",
            ),
        ),
    ] {
        let actual = match session.begin_copy_out(sql).await {
            Ok(_) => Outcome::Ok,
            Err(error) => error.into(),
        };
        assert!(actual == expected, "{sql}");
    }
    // A synthesised table is copied out for real: the rewritten `SELECT` reads
    // it the way every other query does.
    let stream = session
        .begin_copy_out("COPY pg_class TO STDOUT")
        .await
        .expect("a synthesised table copies out")
        .expect("a relation copy is a copy-out");
    assert!(!stream.rows.is_empty());
    check(&[("COPY pg_settings FROM STDIN", {
        Outcome::Failed {
            code: "42809".to_string(),
            message: "cannot copy to view \"pg_settings\"".to_string(),
            detail: None,
            hint: Some(
                "To enable copying to a view, provide an INSTEAD OF INSERT trigger.".to_string(),
            ),
        }
    })])
    .await;
}

/// `ALTER INDEX` on a relation that is not one reports the kind rather than
/// the index catalog's "no such index" — which is the same shared machinery,
/// reached without naming a synthesised relation.
#[tokio::test]
async fn alter_index_names_the_kind_rather_than_reporting_no_index() {
    check(&[
        (
            "ALTER INDEX st SET TABLESPACE pg_default",
            refused("\"st\" is not an index"),
        ),
        (
            "ALTER INDEX sv SET TABLESPACE pg_default",
            refused("\"sv\" is not an index"),
        ),
        (
            "ALTER INDEX ss SET TABLESPACE pg_default",
            refused("\"ss\" is not an index"),
        ),
        (
            "ALTER INDEX pg_settings SET TABLESPACE pg_default",
            refused("\"pg_settings\" is not an index"),
        ),
        (
            "ALTER INDEX pg_class SET TABLESPACE pg_default",
            system_catalog("pg_class"),
        ),
        // A name no relation holds keeps the index catalog's own report.
        (
            "ALTER INDEX nosuch SET TABLESPACE pg_default",
            failed("42704", "index \"nosuch\" does not exist"),
        ),
    ])
    .await;
}

/// `DROP FOREIGN TABLE` shares the ordinary table catalog key, and without a
/// kind test it *dropped* an ordinary table and a materialized view. Naming no
/// synthesised relation at all.
#[tokio::test]
async fn drop_foreign_table_refuses_a_relation_that_is_not_one_and_leaves_it_there() {
    check(&[
        (
            "DROP FOREIGN TABLE st",
            wrong_drop(
                "\"st\" is not a foreign table",
                "Use DROP TABLE to remove a table.",
            ),
        ),
        (
            "DROP FOREIGN TABLE smv",
            wrong_drop(
                "\"smv\" is not a foreign table",
                "Use DROP MATERIALIZED VIEW to remove a materialized view.",
            ),
        ),
        (
            "DROP FOREIGN TABLE sv",
            wrong_drop(
                "\"sv\" is not a foreign table",
                "Use DROP VIEW to remove a view.",
            ),
        ),
        // The refused relations are still there, with their rows.
        ("SELECT count(*) FROM st", Outcome::Ok),
        ("SELECT count(*) FROM smv", Outcome::Ok),
    ])
    .await;
    let (_engine, mut session) = fixture().await;
    assert!(session.simple_query("DROP FOREIGN TABLE st").await.is_err());
    let rows = session
        .simple_query("SELECT i FROM st")
        .await
        .expect("the table survives the refused drop");
    assert!(!rows.is_empty());
}

/// `INHERITS` takes only a table or a foreign table. A materialized view is
/// stored under the table key here, so the clause read one and built a child of
/// it — a relation `PostgreSQL` refuses outright. Naming no synthesised
/// relation.
#[tokio::test]
async fn inherits_refuses_a_parent_that_is_not_a_table() {
    check(&[
        (
            "CREATE TABLE c1 () INHERITS (smv)",
            refused("inherited relation \"smv\" is not a table or foreign table"),
        ),
        (
            "CREATE TABLE c2 () INHERITS (sv)",
            refused("inherited relation \"sv\" is not a table or foreign table"),
        ),
        (
            "CREATE TABLE c3 () INHERITS (ss)",
            refused("inherited relation \"ss\" is not a table or foreign table"),
        ),
        (
            "CREATE TABLE c4 () INHERITS (st_idx)",
            refused_for("cannot open relation \"st_idx\"", "indexes"),
        ),
        // A table parent still works, and a name no relation holds still
        // reports the relation as missing.
        ("CREATE TABLE c5 () INHERITS (st)", Outcome::Ok),
        (
            "CREATE TABLE c6 () INHERITS (nosuch)",
            failed("42P01", "relation \"nosuch\" does not exist"),
        ),
    ])
    .await;
    let (_engine, mut session) = fixture().await;
    assert!(
        session
            .simple_query("CREATE TABLE c1 () INHERITS (smv)")
            .await
            .is_err()
    );
    // The refused statement created nothing.
    assert!(session.simple_query("SELECT * FROM c1").await.is_err());
}

/// `CREATE INDEX` and a foreign key both refuse a relation that cannot carry
/// what they would add, and both name the kind. Naming no synthesised relation.
#[tokio::test]
async fn create_index_and_foreign_key_name_the_kind_they_found() {
    check(&[
        (
            "CREATE INDEX ON sv (i)",
            refused_for("cannot create index on relation \"sv\"", "views"),
        ),
        (
            "CREATE INDEX ON ss (i)",
            refused_for("cannot create index on relation \"ss\"", "sequences"),
        ),
        (
            "CREATE INDEX ON st_idx (i)",
            refused_for("cannot open relation \"st_idx\"", "indexes"),
        ),
        (
            "CREATE TABLE fk1 (a int REFERENCES smv(i))",
            refused("referenced relation \"smv\" is not a table"),
        ),
        (
            "CREATE TABLE fk2 (a int REFERENCES st_idx(i))",
            refused_for("cannot open relation \"st_idx\"", "indexes"),
        ),
        // A materialized view still takes an index, which is what keeps this
        // from being "anything that is not a table".
        ("CREATE INDEX ON smv (i)", Outcome::Ok),
    ])
    .await;
}
