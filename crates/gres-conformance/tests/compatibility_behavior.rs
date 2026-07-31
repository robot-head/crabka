use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use crabka_gres_conformance::{
    feature_manifest::{FEATURE_PROBES, FeatureBehavior},
    parser_command_report,
};
use crabka_pgexec::{
    ExecError, SqlEngine,
    foreign::{ForeignScanner, ImportFilter, ImportedTable, ScanBounds},
};
use crabka_pgtypes::Datum;
use crabka_pgwire::engine::{BoundParam, Engine, Session};

struct EmptyImporter;

impl ForeignScanner for EmptyImporter {
    fn scan(
        &self,
        _table: &crabka_pgcatalog::Table,
        _server: &crabka_pgcatalog::ForeignServer,
        _mapping: Option<&crabka_pgcatalog::UserMapping>,
        _bounds: &ScanBounds,
        _ctx: &crabka_pgexec::clock::EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        Ok(Vec::new())
    }

    fn import_schema(
        &self,
        _server: &crabka_pgcatalog::ForeignServer,
        _mapping: Option<&crabka_pgcatalog::UserMapping>,
        _filter: &ImportFilter,
    ) -> Result<Vec<ImportedTable>, ExecError> {
        Ok(Vec::new())
    }
}

/// The statements a probe needs run before it, so a lifecycle probe can name
/// an object that exists. Shared by the execute and refuse paths: a refusal
/// probe may also need its object present to reach the refusal at all.
fn probe_setup(command: &str) -> &'static [&'static str] {
    match command {
        "ALTER TABLE" | "COMMENT" | "CREATE INDEX" | "CREATE TABLE AS" | "DELETE"
        | "DROP INDEX" | "DROP TABLE" | "INSERT" | "MERGE" | "SELECT INTO" | "TABLE"
        | "TRUNCATE" | "UPDATE" => &["CREATE TABLE parser_commands_probe (id int4)"],
        "GRANT" | "REVOKE" => &[
            "CREATE TABLE parser_commands_probe (id int4)",
            "CREATE ROLE parser_commands_role",
        ],
        "CREATE SERVER" | "DROP FOREIGN DATA WRAPPER" => {
            &["CREATE FOREIGN DATA WRAPPER parser_commands_wrapper"]
        }
        "CREATE FOREIGN TABLE"
        | "CREATE USER MAPPING"
        | "DROP SERVER"
        | "IMPORT FOREIGN SCHEMA" => &[
            "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
        ],
        "DROP FOREIGN TABLE" => &[
            "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE FOREIGN TABLE parser_commands_foreign (id int4) SERVER parser_commands_server",
        ],
        "DROP ROLE" | "SET ROLE" | "SET SESSION AUTHORIZATION" => {
            &["CREATE ROLE parser_commands_role"]
        }
        "ANALYZE" | "REINDEX" | "EXPLAIN" | "CREATE STATISTICS" => {
            &["CREATE TABLE parser_commands_probe (id int4)"]
        }
        "LOCK" | "DECLARE" => &["CREATE TABLE parser_commands_probe (id int4)", "BEGIN"],
        "ROLLBACK TO SAVEPOINT" | "RELEASE SAVEPOINT" => {
            &["BEGIN", "SAVEPOINT parser_commands_savepoint"]
        }
        "FETCH" | "MOVE" | "CLOSE" => &[
            "CREATE TABLE parser_commands_probe (id int4)",
            "BEGIN",
            "DECLARE parser_commands_cursor CURSOR FOR SELECT id FROM parser_commands_probe",
        ],
        "EXECUTE" | "DEALLOCATE" => &["PREPARE parser_commands_prepared AS SELECT 1"],
        "DROP SEQUENCE" => &["CREATE SEQUENCE parser_commands_probe_sequence"],
        "DROP USER" => &["CREATE USER parser_commands_user"],
        "DROP USER MAPPING" => &[
            "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        ],
        "DROP VIEW" => &["CREATE VIEW parser_commands_view AS SELECT 1"],
        // P2: routine lifecycle probes need the routine they name.
        "ALTER FUNCTION" | "ALTER ROUTINE" | "DROP FUNCTION" | "DROP ROUTINE" => {
            &["CREATE FUNCTION parser_commands_fn(a int) RETURNS int AS 'SELECT $1' LANGUAGE sql"]
        }
        "ALTER PROCEDURE" | "DROP PROCEDURE" | "CALL" => {
            &["CREATE PROCEDURE parser_commands_proc(a int) LANGUAGE sql AS 'SELECT $1'"]
        }
        // T5/D7: the probes are executed in command-name order, so every
        // lifecycle probe must create the object it names — `ALTER DOMAIN`
        // sorts ahead of `CREATE DOMAIN`.
        "ALTER TYPE" | "DROP TYPE" => &["CREATE TYPE parser_commands_enum AS ENUM ('a', 'b')"],
        "ALTER DOMAIN" | "DROP DOMAIN" => {
            &["CREATE DOMAIN parser_commands_domain AS int4 CHECK (VALUE > 0)"]
        }
        "ALTER SCHEMA" | "DROP SCHEMA" => &["CREATE SCHEMA parser_commands_schema"],
        "SAVEPOINT" | "SET TRANSACTION" => &["BEGIN"],
        _ => &[],
    }
}

async fn execute_probe(command: &str, sql: &str) {
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(Arc::new(EmptyImporter));
    let mut session = engine.connect();

    let setup: &[&str] = probe_setup(command);
    for statement in setup {
        session.simple_query(statement).await.expect(statement);
    }

    if command == "COPY" {
        session
            .simple_query("CREATE TABLE parser_commands_probe (id int4)")
            .await
            .expect("COPY table setup");
        session
            .begin_copy_in(sql)
            .await
            .expect("COPY must enter CopyIn mode")
            .expect("COPY response");
        session
            .copy_in(sql, vec![Bytes::from_static(b"1\n")])
            .await
            .expect("COPY representative must execute");
    } else {
        session.simple_query(sql).await.expect(sql);
    }
}

#[tokio::test]
async fn every_resolved_behavior_probe_reaches_the_session_contract() {
    let report = parser_command_report().expect("behavior manifest parses");
    assert!(report.probes.len() == 143);
    let mut executed = 0;
    let mut refused = 0;
    for probe in report.probes {
        if probe.behavior == "session-execute" {
            execute_probe(&probe.command, &probe.sql).await;
            executed += 1;
            continue;
        }

        let engine = SqlEngine::new();
        let mut session = engine.connect();
        // A refusal probe may need its object to exist before the statement can
        // reach the refusal at all: `ALTER SCHEMA` on a missing schema is 3F000,
        // and it is the 0A000 refusal the matrix row claims that must be pinned.
        for statement in probe_setup(&probe.command) {
            session.simple_query(statement).await.expect(statement);
        }
        let error = session
            .simple_query(&probe.sql)
            .await
            .expect_err(&probe.sql);
        assert!(
            Some(error.code.as_str()) == probe.sqlstate,
            "{}",
            probe.command
        );
        assert!(
            error
                .message
                .contains(probe.message_fragment.expect("refusal message")),
            "{}: {error:?}",
            probe.command,
        );
        refused += 1;
    }
    assert!(executed == 88);
    assert!(refused == 55);
}

#[tokio::test]
async fn every_major_feature_probe_matches_its_typed_behavior() {
    assert!(FEATURE_PROBES.len() == 48);
    for probe in FEATURE_PROBES {
        if probe.behavior == FeatureBehavior::ParserRejectPending {
            assert!(
                crabka_pgparser::parse(probe.sql).is_err(),
                "pending feature unexpectedly parses: {}",
                probe.item,
            );
            continue;
        }

        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for setup in probe.setup {
            session.simple_query(setup).await.expect(setup);
        }
        match probe.behavior {
            FeatureBehavior::SessionExecute => {
                session.simple_query(probe.sql).await.expect(probe.item);
            }
            FeatureBehavior::ExtendedExecute => {
                session
                    .parse("feature", probe.sql, &[23])
                    .await
                    .expect(probe.item);
                session
                    .bind(
                        "feature_portal",
                        "feature",
                        &[BoundParam {
                            type_oid: Some(23),
                            format: 0,
                            value: Some(Bytes::from_static(b"7")),
                        }],
                        &[],
                    )
                    .await
                    .expect(probe.item);
                session
                    .execute("feature_portal", 0)
                    .await
                    .expect(probe.item);
            }
            FeatureBehavior::SessionRefuse => {
                let error = session.simple_query(probe.sql).await.expect_err(probe.item);
                assert!(
                    Some(error.code.as_str()) == probe.sqlstate,
                    "{}",
                    probe.item
                );
                assert!(
                    error
                        .message
                        .contains(probe.message_fragment.expect("message")),
                    "{}: {error:?}",
                    probe.item,
                );
            }
            FeatureBehavior::ParserRejectPending => unreachable!(),
        }
    }
}
