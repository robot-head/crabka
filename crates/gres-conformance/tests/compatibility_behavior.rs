use std::sync::Arc;

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

async fn execute_probe(command: &str, sql: &str) {
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(Arc::new(EmptyImporter));
    let mut session = engine.connect();

    let setup: &[&str] = match command {
        "ALTER TABLE" | "CREATE INDEX" | "DELETE" | "DROP INDEX" | "DROP TABLE" | "INSERT"
        | "UPDATE" => &["CREATE TABLE parser_commands_probe (id int4)"],
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
        "DROP ROLE" | "SET ROLE" => &["CREATE ROLE parser_commands_role"],
        "DROP SEQUENCE" => &["CREATE SEQUENCE parser_commands_probe_sequence"],
        "DROP USER" => &["CREATE USER parser_commands_user"],
        "DROP USER MAPPING" => &[
            "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
            "CREATE USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        ],
        "DROP VIEW" => &["CREATE VIEW parser_commands_view AS SELECT 1"],
        "SET TRANSACTION" => &["BEGIN"],
        _ => &[],
    };
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
    assert_eq!(report.probes.len(), 92);
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
        let error = session
            .simple_query(&probe.sql)
            .await
            .expect_err(&probe.sql);
        assert_eq!(
            Some(error.code.as_str()),
            probe.sqlstate,
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
    assert_eq!(executed, 42);
    assert_eq!(refused, 50);
}

#[tokio::test]
async fn every_major_feature_probe_matches_its_typed_behavior() {
    assert_eq!(FEATURE_PROBES.len(), 23);
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
                assert_eq!(Some(error.code.as_str()), probe.sqlstate, "{}", probe.item);
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
