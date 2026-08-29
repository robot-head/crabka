//! Parser-command probes for the `PostgreSQL` compatibility matrix.

use crabka_pgparser::{ParseError, ast::Statement, parse};
use serde::Serialize;
use thiserror::Error;

/// Version of the JSON report emitted by `crabka-gres-parser-commands`.
pub const PARSER_COMMAND_REPORT_FORMAT_VERSION: u32 = 2;

/// Stable machine-readable inventory of SQL commands accepted by the parser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParserCommandReport {
    /// Schema version for consumers of this report.
    pub format_version: u32,
    /// Uppercase `PostgreSQL` command names in lexical order.
    pub commands: Vec<String>,
    /// One bidirectional behavior contract for every resolved command.
    pub probes: Vec<BehaviorProbe>,
    /// Major language features, deliberately separate from command identities.
    pub features: &'static [crate::feature_manifest::FeatureProbe],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BehaviorProbe {
    pub command: String,
    pub sql: String,
    pub parser_shape: String,
    pub behavior: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_fragment: Option<&'static str>,
}

/// Failure while proving that a documented command is accepted by the parser.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParserCommandError {
    /// A representative SQL command was rejected by the public parser API.
    #[error("parser rejected {command} probe `{sql}`: {source}")]
    Rejected {
        command: &'static str,
        sql: &'static str,
        /// Boxed because `ParseError` carries a message, a detail and a hint,
        /// which puts this variant over the 128 bytes `result_large_err`
        /// allows -- and every function here returns this type on a path that
        /// almost always succeeds, so the whole `Result` would pay for the one
        /// rejection that never happens.
        #[source]
        source: Box<ParseError>,
    },
    /// A representative SQL command did not produce exactly one statement.
    #[error("parser command probe {command} produced {count} statements; expected exactly one")]
    StatementCount { command: &'static str, count: usize },
    /// A representative SQL command parsed into a different AST shape.
    #[error("parser command probe {command} produced {actual}; expected {expected}")]
    UnexpectedStatement {
        command: &'static str,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("parser command probe {command} was classified as {actual}")]
    UnexpectedIdentity {
        command: &'static str,
        actual: &'static str,
    },
}

struct CommandProbe {
    command: &'static str,
    sql: &'static str,
    expected_statement: &'static str,
    /// The `(sqlstate, message fragment)` a command refuses with when the
    /// refusal is the executor's rather than a parser-level
    /// [`crabka_pgparser::ast::RefusalCommand`].
    refusal: Option<(&'static str, &'static str)>,
}

const COMMAND_PROBES: &[CommandProbe] = &[
    CommandProbe {
        command: "ALTER LARGE OBJECT",
        sql: "ALTER LARGE OBJECT 4242 OWNER TO postgres",
        expected_statement: "AlterLargeObject",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER DEFAULT PRIVILEGES",
        sql: "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC",
        expected_statement: "AlterDefaultTablePrivileges",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER DATABASE",
        sql: "ALTER DATABASE postgres RENAME TO other",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE DATABASE",
        sql: "CREATE DATABASE other",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "DROP DATABASE",
        sql: "DROP DATABASE other",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER EXTENSION",
        sql: "ALTER EXTENSION plpgsql UPDATE",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "DROP EXTENSION",
        sql: "DROP EXTENSION plpgsql",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "PREPARE TRANSACTION",
        sql: "PREPARE TRANSACTION 'xid-1'",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "COMMIT PREPARED",
        sql: "COMMIT PREPARED 'xid-1'",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "ROLLBACK PREPARED",
        sql: "ROLLBACK PREPARED 'xid-1'",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TABLE",
        sql: "CREATE TABLE parser_commands_probe (id int4)",
        expected_statement: "CreateTable",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE ACCESS METHOD",
        sql: "CREATE ACCESS METHOD parser_commands_am TYPE TABLE HANDLER heap_tableam_handler",
        expected_statement: "CreateAccessMethod",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE VIEW",
        sql: "CREATE VIEW parser_commands_view AS SELECT 1",
        expected_statement: "CreateView",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE MATERIALIZED VIEW",
        sql: "CREATE MATERIALIZED VIEW parser_commands_matview AS SELECT 1",
        expected_statement: "CreateMaterializedView",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER VIEW",
        sql: "ALTER VIEW parser_commands_view SET (security_invoker = true)",
        expected_statement: "AlterView",
        refusal: None,
    },
    CommandProbe {
        // Routed onto the `ALTER TABLE` action set, so the whole ordinary
        // subcommand family applies to a materialized view for free.
        command: "ALTER MATERIALIZED VIEW",
        sql: "ALTER MATERIALIZED VIEW parser_commands_matview OWNER TO postgres",
        expected_statement: "AlterTable",
        refusal: None,
    },
    CommandProbe {
        command: "REFRESH MATERIALIZED VIEW",
        sql: "REFRESH MATERIALIZED VIEW parser_commands_matview",
        expected_statement: "RefreshMaterializedView",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TABLE ENABLE ROW LEVEL SECURITY",
        sql: "ALTER TABLE parser_commands_probe ENABLE ROW LEVEL SECURITY",
        expected_statement: "AlterTable",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE POLICY",
        sql: "CREATE POLICY parser_commands_policy ON parser_commands_probe FOR SELECT TO PUBLIC USING (true)",
        expected_statement: "CreatePolicy",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER POLICY",
        sql: "ALTER POLICY parser_commands_policy ON parser_commands_probe USING (false)",
        expected_statement: "AlterPolicy",
        refusal: None,
    },
    CommandProbe {
        command: "DROP POLICY",
        sql: "DROP POLICY parser_commands_policy ON parser_commands_probe",
        expected_statement: "DropPolicy",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TRIGGER",
        sql: "CREATE TRIGGER parser_commands_trigger BEFORE INSERT ON parser_commands_probe FOR EACH ROW EXECUTE FUNCTION parser_commands_trigger_fn()",
        expected_statement: "CreateTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TRIGGER",
        sql: "ALTER TRIGGER parser_commands_trigger ON parser_commands_probe RENAME TO parser_commands_trigger_renamed",
        expected_statement: "AlterTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TRIGGER",
        sql: "DROP TRIGGER parser_commands_trigger ON parser_commands_probe",
        expected_statement: "DropTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE RULE",
        sql: "CREATE RULE parser_commands_rule AS ON INSERT TO parser_commands_probe DO INSTEAD NOTHING",
        expected_statement: "CreateRule",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER RULE",
        sql: "ALTER RULE parser_commands_rule ON parser_commands_probe RENAME TO parser_commands_rule_renamed",
        expected_statement: "AlterRule",
        refusal: None,
    },
    CommandProbe {
        command: "DROP RULE",
        sql: "DROP RULE parser_commands_rule ON parser_commands_probe",
        expected_statement: "DropRule",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE EVENT TRIGGER",
        sql: "CREATE EVENT TRIGGER parser_commands_event_trigger ON ddl_command_start EXECUTE FUNCTION parser_commands_event_trigger_fn()",
        expected_statement: "CreateEventTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER EVENT TRIGGER",
        sql: "ALTER EVENT TRIGGER parser_commands_event_trigger ENABLE",
        expected_statement: "AlterEventTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "DROP EVENT TRIGGER",
        sql: "DROP EVENT TRIGGER parser_commands_event_trigger",
        expected_statement: "DropEventTrigger",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE INDEX",
        sql: "CREATE INDEX parser_commands_probe_index ON parser_commands_probe (id)",
        expected_statement: "CreateIndex",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE SEQUENCE",
        sql: "CREATE SEQUENCE parser_commands_probe_sequence",
        expected_statement: "CreateSequence",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TABLE",
        sql: "DROP TABLE parser_commands_probe",
        expected_statement: "DropTable",
        refusal: None,
    },
    CommandProbe {
        command: "DROP VIEW",
        sql: "DROP VIEW parser_commands_view",
        expected_statement: "DropView",
        refusal: None,
    },
    CommandProbe {
        command: "DROP MATERIALIZED VIEW",
        sql: "DROP MATERIALIZED VIEW parser_commands_matview",
        expected_statement: "DropMaterializedView",
        refusal: None,
    },
    CommandProbe {
        command: "DROP INDEX",
        sql: "DROP INDEX IF EXISTS parser_commands_probe_index",
        expected_statement: "DropIndex",
        refusal: None,
    },
    CommandProbe {
        command: "DROP SEQUENCE",
        sql: "DROP SEQUENCE parser_commands_probe_sequence",
        expected_statement: "DropSequence",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TABLE",
        sql: "ALTER TABLE parser_commands_probe RENAME TO parser_commands_renamed_probe",
        expected_statement: "AlterTable",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER INDEX",
        sql: "ALTER INDEX parser_commands_idx SET TABLESPACE pg_default",
        expected_statement: "AlterIndex",
        refusal: None,
    },
    CommandProbe {
        command: "COMMENT",
        sql: "COMMENT ON TABLE parser_commands_probe IS 'probe comment'",
        expected_statement: "Comment",
        refusal: None,
    },
    CommandProbe {
        command: "INSERT",
        sql: "INSERT INTO parser_commands_probe VALUES (1)",
        expected_statement: "Insert",
        refusal: None,
    },
    CommandProbe {
        command: "TRUNCATE",
        sql: "TRUNCATE parser_commands_probe",
        expected_statement: "Truncate",
        refusal: None,
    },
    CommandProbe {
        command: "LISTEN",
        sql: "LISTEN parser_commands_channel",
        expected_statement: "Listen",
        refusal: None,
    },
    CommandProbe {
        command: "NOTIFY",
        sql: "NOTIFY parser_commands_channel, 'parser commands payload'",
        expected_statement: "Notify",
        refusal: None,
    },
    CommandProbe {
        command: "UNLISTEN",
        sql: "UNLISTEN parser_commands_channel",
        expected_statement: "Unlisten",
        refusal: None,
    },
    CommandProbe {
        command: "VACUUM",
        sql: "VACUUM ANALYZE parser_commands_probe",
        expected_statement: "Vacuum",
        refusal: None,
    },
    CommandProbe {
        command: "SELECT",
        sql: "SELECT 1",
        expected_statement: "Query",
        refusal: None,
    },
    CommandProbe {
        command: "VALUES",
        sql: "VALUES (1)",
        expected_statement: "Query",
        refusal: None,
    },
    CommandProbe {
        command: "BEGIN",
        sql: "BEGIN",
        expected_statement: "Begin",
        refusal: None,
    },
    CommandProbe {
        command: "START TRANSACTION",
        sql: "START TRANSACTION",
        expected_statement: "Begin",
        refusal: None,
    },
    CommandProbe {
        command: "COMMIT",
        sql: "COMMIT",
        expected_statement: "Commit",
        refusal: None,
    },
    CommandProbe {
        command: "END",
        sql: "END",
        expected_statement: "Commit",
        refusal: None,
    },
    CommandProbe {
        command: "ROLLBACK",
        sql: "ROLLBACK",
        expected_statement: "Rollback",
        refusal: None,
    },
    CommandProbe {
        command: "ABORT",
        sql: "ABORT",
        expected_statement: "Rollback",
        refusal: None,
    },
    CommandProbe {
        command: "UPDATE",
        sql: "UPDATE parser_commands_probe SET id = 1",
        expected_statement: "Update",
        refusal: None,
    },
    CommandProbe {
        command: "DELETE",
        sql: "DELETE FROM parser_commands_probe",
        expected_statement: "Delete",
        refusal: None,
    },
    CommandProbe {
        command: "MERGE",
        sql: "MERGE INTO parser_commands_probe AS t USING parser_commands_probe AS s ON t.id = s.id WHEN MATCHED THEN DO NOTHING",
        expected_statement: "Merge",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TABLE AS",
        sql: "CREATE TABLE parser_commands_ctas_probe AS SELECT id FROM parser_commands_probe",
        expected_statement: "CreateTableAs",
        refusal: None,
    },
    CommandProbe {
        command: "SELECT INTO",
        sql: "SELECT id INTO parser_commands_into_probe FROM parser_commands_probe",
        expected_statement: "CreateTableAs",
        refusal: None,
    },
    CommandProbe {
        command: "TABLE",
        sql: "TABLE parser_commands_probe",
        expected_statement: "Query",
        refusal: None,
    },
    CommandProbe {
        command: "SET",
        sql: "SET extra_float_digits TO 2",
        expected_statement: "Set",
        refusal: None,
    },
    CommandProbe {
        command: "SET TRANSACTION",
        sql: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        expected_statement: "SetTransaction",
        refusal: None,
    },
    CommandProbe {
        command: "SHOW",
        sql: "SHOW extra_float_digits",
        expected_statement: "Show",
        refusal: None,
    },
    CommandProbe {
        command: "RESET",
        sql: "RESET extra_float_digits",
        expected_statement: "Reset",
        refusal: None,
    },
    CommandProbe {
        command: "DISCARD",
        sql: "DISCARD ALL",
        expected_statement: "Discard",
        refusal: None,
    },
    CommandProbe {
        command: "COPY",
        sql: "COPY parser_commands_probe FROM STDIN",
        expected_statement: "CopyFromStdin",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE FOREIGN DATA WRAPPER",
        sql: "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "CreateFdw",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER FOREIGN DATA WRAPPER",
        sql: "ALTER FOREIGN DATA WRAPPER parser_commands_wrapper OPTIONS (host 'localhost')",
        expected_statement: "AlterFdw",
        refusal: None,
    },
    CommandProbe {
        command: "DROP FOREIGN DATA WRAPPER",
        sql: "DROP FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "DropFdw",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE SERVER",
        sql: "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "CreateServer",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER SERVER",
        sql: "ALTER SERVER parser_commands_server OPTIONS (host 'localhost')",
        expected_statement: "AlterServer",
        refusal: None,
    },
    CommandProbe {
        command: "DROP SERVER",
        sql: "DROP SERVER parser_commands_server",
        expected_statement: "DropServer",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE USER MAPPING",
        sql: "CREATE USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        expected_statement: "CreateUserMapping",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER USER MAPPING",
        sql: "ALTER USER MAPPING FOR PUBLIC SERVER parser_commands_server OPTIONS (username 'crab')",
        expected_statement: "AlterUserMapping",
        refusal: None,
    },
    CommandProbe {
        command: "DROP USER MAPPING",
        sql: "DROP USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        expected_statement: "DropUserMapping",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE FOREIGN TABLE",
        sql: "CREATE FOREIGN TABLE parser_commands_foreign (id int4) SERVER parser_commands_server",
        expected_statement: "CreateForeignTable",
        refusal: None,
    },
    CommandProbe {
        command: "DROP FOREIGN TABLE",
        sql: "DROP FOREIGN TABLE parser_commands_foreign",
        expected_statement: "DropForeignTable",
        refusal: None,
    },
    CommandProbe {
        command: "IMPORT FOREIGN SCHEMA",
        sql: "IMPORT FOREIGN SCHEMA parser_commands_schema FROM SERVER parser_commands_server",
        expected_statement: "ImportForeignSchema",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE ROLE",
        sql: "CREATE ROLE parser_commands_role",
        expected_statement: "CreateRole",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE USER",
        sql: "CREATE USER parser_commands_user",
        expected_statement: "CreateRole",
        refusal: None,
    },
    CommandProbe {
        command: "DROP ROLE",
        sql: "DROP ROLE parser_commands_role",
        expected_statement: "DropRole",
        refusal: None,
    },
    CommandProbe {
        command: "DROP USER",
        sql: "DROP USER parser_commands_user",
        expected_statement: "DropRole",
        refusal: None,
    },
    CommandProbe {
        command: "GRANT",
        sql: "GRANT SELECT ON TABLE parser_commands_probe TO parser_commands_role",
        expected_statement: "GrantTablePrivileges",
        refusal: None,
    },
    CommandProbe {
        command: "REVOKE",
        sql: "REVOKE SELECT ON TABLE parser_commands_probe FROM parser_commands_role",
        expected_statement: "RevokeTablePrivileges",
        refusal: None,
    },
    CommandProbe {
        command: "SET ROLE",
        sql: "SET ROLE parser_commands_role",
        expected_statement: "SetRole",
        refusal: None,
    },
    CommandProbe {
        command: "SAVEPOINT",
        sql: "SAVEPOINT parser_commands_savepoint",
        expected_statement: "Savepoint",
        refusal: None,
    },
    CommandProbe {
        command: "ROLLBACK TO SAVEPOINT",
        sql: "ROLLBACK TO SAVEPOINT parser_commands_savepoint",
        expected_statement: "RollbackToSavepoint",
        refusal: None,
    },
    CommandProbe {
        command: "RELEASE SAVEPOINT",
        sql: "RELEASE SAVEPOINT parser_commands_savepoint",
        expected_statement: "ReleaseSavepoint",
        refusal: None,
    },
    CommandProbe {
        command: "DECLARE",
        sql: "DECLARE parser_commands_cursor CURSOR FOR SELECT id FROM parser_commands_probe",
        expected_statement: "DeclareCursor",
        refusal: None,
    },
    CommandProbe {
        command: "FETCH",
        sql: "FETCH ALL FROM parser_commands_cursor",
        expected_statement: "FetchCursor",
        refusal: None,
    },
    CommandProbe {
        command: "MOVE",
        sql: "MOVE ALL IN parser_commands_cursor",
        expected_statement: "FetchCursor",
        refusal: None,
    },
    CommandProbe {
        command: "CLOSE",
        sql: "CLOSE parser_commands_cursor",
        expected_statement: "CloseCursor",
        refusal: None,
    },
    CommandProbe {
        command: "PREPARE",
        sql: "PREPARE parser_commands_prepared AS SELECT 1",
        expected_statement: "PrepareStatement",
        refusal: None,
    },
    CommandProbe {
        command: "EXECUTE",
        sql: "EXECUTE parser_commands_prepared",
        expected_statement: "ExecuteStatement",
        refusal: None,
    },
    CommandProbe {
        command: "DEALLOCATE",
        sql: "DEALLOCATE parser_commands_prepared",
        expected_statement: "Deallocate",
        refusal: None,
    },
    CommandProbe {
        command: "LOCK",
        sql: "LOCK TABLE parser_commands_probe IN ACCESS SHARE MODE",
        expected_statement: "LockTable",
        refusal: None,
    },
    CommandProbe {
        command: "EXPLAIN",
        sql: "EXPLAIN (COSTS OFF) SELECT 1",
        expected_statement: "Explain",
        refusal: None,
    },
    CommandProbe {
        command: "ANALYZE",
        sql: "ANALYZE parser_commands_probe",
        expected_statement: "Analyze",
        refusal: None,
    },
    CommandProbe {
        command: "CLUSTER",
        sql: "CLUSTER",
        expected_statement: "Cluster",
        refusal: None,
    },
    CommandProbe {
        command: "REINDEX",
        sql: "REINDEX TABLE parser_commands_probe",
        expected_statement: "Reindex",
        refusal: None,
    },
    CommandProbe {
        command: "CHECKPOINT",
        sql: "CHECKPOINT",
        expected_statement: "Checkpoint",
        refusal: None,
    },
    CommandProbe {
        command: "LOAD",
        sql: "LOAD 'regress'",
        expected_statement: "Load",
        refusal: None,
    },
    CommandProbe {
        command: "SECURITY LABEL",
        sql: "SECURITY LABEL ON TABLE parser_commands_probe IS 'classified'",
        expected_statement: "SecurityLabel",
        refusal: Some(("22023", "no security label providers have been loaded")),
    },
    CommandProbe {
        command: "CREATE TABLESPACE",
        sql: "CREATE TABLESPACE parser_commands_space LOCATION '/tmp/parser_commands_space'",
        expected_statement: "CreateTablespace",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TABLESPACE",
        sql: "ALTER TABLESPACE parser_commands_space RENAME TO parser_commands_space2",
        expected_statement: "AlterTablespace",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TABLESPACE",
        sql: "DROP TABLESPACE IF EXISTS parser_commands_space",
        expected_statement: "DropTablespace",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE OPERATOR CLASS",
        sql: "CREATE OPERATOR CLASS parser_commands_ops FOR TYPE uuid USING hash AS STORAGE uuid",
        expected_statement: "CreateOperatorClass",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TABLE INHERITS",
        sql: "CREATE TABLE parser_commands_child (extra int4) INHERITS (parser_commands_parent)",
        expected_statement: "CreateTable",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE OPERATOR FAMILY",
        sql: "CREATE OPERATOR FAMILY parser_commands_family USING hash",
        expected_statement: "CreateOperatorFamily",
        refusal: None,
    },
    // A binary-coercible pair: both are 8 bytes and Gres can decode one as the
    // other, which is what `CREATE CAST ... WITHOUT FUNCTION` asserts. The pair
    // is probed at DDL time rather than at first use, so a pair that cannot be
    // decoded is refused here rather than surfacing as a wrong value later.
    CommandProbe {
        command: "CREATE CAST",
        sql: "CREATE CAST (int8 AS timestamp) WITHOUT FUNCTION",
        expected_statement: "CreateCast",
        refusal: None,
    },
    CommandProbe {
        command: "DROP CAST",
        sql: "DROP CAST (int8 AS timestamp)",
        expected_statement: "DropCast",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER OPERATOR CLASS",
        sql: "ALTER OPERATOR CLASS parser_commands_ops USING hash RENAME TO parser_commands_ops2",
        expected_statement: "AlterOperatorObject",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER OPERATOR FAMILY",
        sql: "ALTER OPERATOR FAMILY parser_commands_family USING hash RENAME TO parser_commands_family2",
        expected_statement: "AlterOperatorObject",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE OPERATOR",
        sql: "CREATE OPERATOR === (PROCEDURE = int8eq, LEFTARG = bigint, RIGHTARG = bigint)",
        expected_statement: "CreateOperator",
        refusal: None,
    },
    // `IF EXISTS` so the probe needs no setup of its own: the notice path is a
    // documented success, and the lifecycle pairing with the `CREATE OPERATOR`
    // probe above is covered by the session tests rather than here.
    CommandProbe {
        command: "DROP OPERATOR",
        sql: "DROP OPERATOR IF EXISTS ===(bigint, bigint)",
        expected_statement: "DropOperator",
        refusal: None,
    },
    CommandProbe {
        command: "DROP OPERATOR CLASS",
        sql: "DROP OPERATOR CLASS IF EXISTS parser_commands_ops USING hash",
        expected_statement: "DropOperatorObject",
        refusal: None,
    },
    CommandProbe {
        command: "DROP OPERATOR FAMILY",
        sql: "DROP OPERATOR FAMILY IF EXISTS parser_commands_family USING hash",
        expected_statement: "DropOperatorObject",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER SYSTEM",
        sql: "ALTER SYSTEM RESET work_mem",
        expected_statement: "AlterSystem",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TEXT SEARCH CONFIGURATION",
        sql: "CREATE TEXT SEARCH CONFIGURATION parser_commands_cfg (COPY = english)",
        expected_statement: "CreateTextSearchConfiguration",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TEXT SEARCH CONFIGURATION",
        sql: "ALTER TEXT SEARCH CONFIGURATION english ADD MAPPING FOR asciiword WITH simple",
        expected_statement: "AlterTextSearchConfiguration",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TEXT SEARCH CONFIGURATION",
        sql: "DROP TEXT SEARCH CONFIGURATION IF EXISTS parser_commands_missing_cfg",
        expected_statement: "DropTextSearchConfiguration",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE TEXT SEARCH DICTIONARY",
        sql: "CREATE TEXT SEARCH DICTIONARY parser_commands_dict (TEMPLATE = simple)",
        expected_statement: "CreateTextSearchDictionary",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TEXT SEARCH DICTIONARY",
        sql: "ALTER TEXT SEARCH DICTIONARY simple (STOPWORDS = english)",
        expected_statement: "AlterTextSearchDictionary",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TEXT SEARCH DICTIONARY",
        sql: "DROP TEXT SEARCH DICTIONARY IF EXISTS parser_commands_missing_dict",
        expected_statement: "DropTextSearchDictionary",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE STATISTICS",
        sql: "CREATE STATISTICS parser_commands_stats ON id FROM parser_commands_probe",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER STATISTICS",
        sql: "ALTER STATISTICS parser_commands_stats SET STATISTICS 100",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "DROP STATISTICS",
        sql: "DROP STATISTICS parser_commands_stats",
        expected_statement: "CompatibilityRefusal",
        refusal: None,
    },
    CommandProbe {
        command: "SET CONSTRAINTS",
        sql: "SET CONSTRAINTS ALL IMMEDIATE",
        expected_statement: "SetConstraints",
        refusal: None,
    },
    CommandProbe {
        command: "SET SESSION AUTHORIZATION",
        sql: "SET SESSION AUTHORIZATION DEFAULT",
        expected_statement: "SetSessionAuthorization",
        refusal: None,
    },
    // D7: schemas. A schema is recorded but not yet usable as a namespace —
    // see the CREATE SCHEMA matrix row for that divergence.
    CommandProbe {
        command: "CREATE SCHEMA",
        sql: "CREATE SCHEMA parser_commands_schema",
        expected_statement: "CreateSchema",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER SCHEMA",
        sql: "ALTER SCHEMA parser_commands_schema OWNER TO postgres",
        expected_statement: "AlterSchema",
        refusal: None,
    },
    CommandProbe {
        command: "DROP SCHEMA",
        sql: "DROP SCHEMA parser_commands_schema",
        expected_statement: "DropSchema",
        refusal: None,
    },
    // T5: user-defined types.
    CommandProbe {
        command: "CREATE TYPE",
        sql: "CREATE TYPE parser_commands_enum AS ENUM ('a', 'b')",
        expected_statement: "CreateType",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER ROLE",
        sql: "ALTER ROLE parser_commands_role WITH NOSUPERUSER",
        expected_statement: "AlterRole",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER TYPE",
        sql: "ALTER TYPE parser_commands_enum ADD VALUE 'c'",
        expected_statement: "AlterType",
        refusal: None,
    },
    CommandProbe {
        command: "DROP TYPE",
        sql: "DROP TYPE parser_commands_enum",
        expected_statement: "DropType",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE DOMAIN",
        sql: "CREATE DOMAIN parser_commands_domain AS int4 CHECK (VALUE > 0)",
        expected_statement: "CreateDomain",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER DOMAIN",
        sql: "ALTER DOMAIN parser_commands_domain SET DEFAULT 1",
        expected_statement: "AlterDomain",
        refusal: None,
    },
    CommandProbe {
        command: "DROP DOMAIN",
        sql: "DROP DOMAIN parser_commands_domain",
        expected_statement: "DropDomain",
        refusal: None,
    },
    // P2: SQL routines.
    CommandProbe {
        command: "CREATE FUNCTION",
        sql: "CREATE FUNCTION parser_commands_fn(a int) RETURNS int AS 'SELECT $1' LANGUAGE sql",
        expected_statement: "CreateRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "CREATE PROCEDURE",
        sql: "CREATE PROCEDURE parser_commands_proc(a int) LANGUAGE sql AS 'SELECT $1'",
        expected_statement: "CreateRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER FUNCTION",
        sql: "ALTER FUNCTION parser_commands_fn(int) IMMUTABLE",
        expected_statement: "AlterRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER PROCEDURE",
        sql: "ALTER PROCEDURE parser_commands_proc(int) SECURITY DEFINER",
        expected_statement: "AlterRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER ROUTINE",
        sql: "ALTER ROUTINE parser_commands_fn(int) RENAME TO parser_commands_fn2",
        expected_statement: "AlterRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "DROP FUNCTION",
        sql: "DROP FUNCTION parser_commands_fn(int)",
        expected_statement: "DropRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "DROP PROCEDURE",
        sql: "DROP PROCEDURE parser_commands_proc(int)",
        expected_statement: "DropRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "DROP ROUTINE",
        sql: "DROP ROUTINE parser_commands_fn(int)",
        expected_statement: "DropRoutine",
        refusal: None,
    },
    CommandProbe {
        command: "CALL",
        sql: "CALL parser_commands_proc(1)",
        expected_statement: "Call",
        refusal: None,
    },
    // The default language is PL/pgSQL, whose inline interpreter executes the
    // ordinary empty block used by this parser and behavior probe.
    CommandProbe {
        command: "DO",
        sql: "DO $$ BEGIN NULL; END $$",
        expected_statement: "DoBlock",
        refusal: None,
    },
    // User-defined aggregates, which are routines with `prokind = 'a'`: their
    // transition function is an ordinary SQL function the setup defines first.
    CommandProbe {
        command: "CREATE AGGREGATE",
        sql: "CREATE AGGREGATE parser_commands_agg (int4) (SFUNC = parser_commands_add, STYPE = \
              int4, INITCOND = '0')",
        expected_statement: "CreateAggregate",
        refusal: None,
    },
    CommandProbe {
        command: "ALTER AGGREGATE",
        sql: "ALTER AGGREGATE parser_commands_agg (int4) OWNER TO postgres",
        expected_statement: "AlterAggregate",
        refusal: None,
    },
    CommandProbe {
        command: "DROP AGGREGATE",
        sql: "DROP AGGREGATE parser_commands_agg (int4)",
        expected_statement: "DropAggregate",
        refusal: None,
    },
];

/// Build the compatibility matrix's parser-command inventory.
///
/// Every command has a representative SQL probe and is parsed through
/// [`crabka_pgparser::parse`]. The AST shape check makes changes to parser
/// dispatch explicit while an exhaustive [`Statement`] match forces this module
/// to account for new statement variants.
///
/// # Errors
///
/// Returns an error when a probe is rejected, produces multiple statements, or
/// maps to an unexpected AST shape.
pub fn parser_command_report() -> Result<ParserCommandReport, ParserCommandError> {
    let commands = crabka_pgparser::command::CommandIdentity::ALL
        .iter()
        .map(|identity| identity.name().to_string())
        .collect();
    let mut probes =
        Vec::with_capacity(COMMAND_PROBES.len() + crabka_pgparser::ast::NON_GOAL_REFUSALS.len());
    for probe in COMMAND_PROBES {
        validate_probe(probe)?;
        probes.push(behavior_probe(probe)?);
    }
    for spec in crabka_pgparser::ast::NON_GOAL_REFUSALS {
        let probe = CommandProbe {
            command: spec.command.command_name(),
            sql: spec.representative_sql,
            expected_statement: "CompatibilityRefusal",
            refusal: None,
        };
        validate_probe(&probe)?;
        probes.push(behavior_probe(&probe)?);
    }
    probes.sort_unstable_by(|left, right| left.command.cmp(&right.command));

    Ok(ParserCommandReport {
        format_version: PARSER_COMMAND_REPORT_FORMAT_VERSION,
        commands,
        probes,
        features: crate::feature_manifest::FEATURE_PROBES,
    })
}

fn behavior_probe(probe: &CommandProbe) -> Result<BehaviorProbe, ParserCommandError> {
    let statements = parse(probe.sql).map_err(|source| ParserCommandError::Rejected {
        command: probe.command,
        sql: probe.sql,
        source: Box::new(source),
    })?;
    let [statement] = statements.as_slice() else {
        return Err(ParserCommandError::StatementCount {
            command: probe.command,
            count: statements.len(),
        });
    };
    let (behavior, sqlstate, message_fragment) =
        if let Some(command) = statement.compatibility_refusal() {
            ("refuse", Some(command.sqlstate()), Some(command.message()))
        } else if let Some((sqlstate, fragment)) = probe.refusal {
            ("refuse", Some(sqlstate), Some(fragment))
        } else {
            ("session-execute", None, None)
        };
    Ok(BehaviorProbe {
        command: probe.command.to_string(),
        sql: probe.sql.to_string(),
        parser_shape: probe.expected_statement.to_string(),
        behavior,
        sqlstate,
        message_fragment,
    })
}

fn validate_probe(probe: &CommandProbe) -> Result<(), ParserCommandError> {
    let classified =
        crabka_pgparser::parse_with_command_identities(probe.sql).map_err(|source| {
            ParserCommandError::Rejected {
                command: probe.command,
                sql: probe.sql,
                source: Box::new(source),
            }
        })?;
    let [(statement, identity)] = classified.as_slice() else {
        return Err(ParserCommandError::StatementCount {
            command: probe.command,
            count: classified.len(),
        });
    };
    if identity.name() != probe.command {
        return Err(ParserCommandError::UnexpectedIdentity {
            command: probe.command,
            actual: identity.name(),
        });
    }

    let actual = statement_shape(statement);
    if actual != probe.expected_statement {
        return Err(ParserCommandError::UnexpectedStatement {
            command: probe.command,
            expected: probe.expected_statement,
            actual,
        });
    }
    Ok(())
}

fn statement_shape(statement: &Statement) -> &'static str {
    match statement {
        Statement::CompatibilityRefusal(_) => "CompatibilityRefusal",
        Statement::CreateTable { .. } => "CreateTable",
        Statement::CreateView { .. } => "CreateView",
        Statement::CreateRule(_) => "CreateRule",
        Statement::AlterRule { .. } => "AlterRule",
        Statement::DropRule { .. } => "DropRule",
        Statement::CreateTrigger(_) => "CreateTrigger",
        Statement::AlterTrigger { .. } => "AlterTrigger",
        Statement::DropTrigger { .. } => "DropTrigger",
        Statement::CreatePolicy(_) => "CreatePolicy",
        Statement::AlterPolicy { .. } => "AlterPolicy",
        Statement::DropPolicy { .. } => "DropPolicy",
        Statement::GrantRoles { .. } => "GrantRoles",
        Statement::RevokeRoles { .. } => "RevokeRoles",
        Statement::CreateEventTrigger(_) => "CreateEventTrigger",
        Statement::AlterEventTrigger { .. } => "AlterEventTrigger",
        Statement::DropEventTrigger { .. } => "DropEventTrigger",
        Statement::CreateRoutine(_) => "CreateRoutine",
        Statement::CreateAggregate(_) => "CreateAggregate",
        Statement::DropAggregate { .. } => "DropAggregate",
        Statement::AlterAggregate { .. } => "AlterAggregate",
        Statement::DropRoutine { .. } => "DropRoutine",
        Statement::AlterRoutine { .. } => "AlterRoutine",
        Statement::Call { .. } => "Call",
        Statement::DoBlock { .. } => "DoBlock",
        Statement::CreateIndex { table, .. } if table.name == "__crabka_sequence__" => {
            "CreateSequence"
        }
        Statement::CreateIndex { .. } => "CreateIndex",
        Statement::DropIndex { .. } => "DropIndex",
        Statement::AlterIndex { .. } => "AlterIndex",
        Statement::AlterView { .. } => "AlterView",
        Statement::DropTable { names, .. }
            if names
                .first()
                .is_some_and(|name| name.name.starts_with("__crabka_sequence__:")) =>
        {
            "DropSequence"
        }
        Statement::DropTable { .. } => "DropTable",
        Statement::DropView { .. } => "DropView",
        Statement::CreateMaterializedView { .. } => "CreateMaterializedView",
        Statement::RefreshMaterializedView { .. } => "RefreshMaterializedView",
        Statement::DropMaterializedView { .. } => "DropMaterializedView",
        Statement::CreateSchema { .. } => "CreateSchema",
        Statement::AlterSchema { .. } => "AlterSchema",
        Statement::DropSchema { .. } => "DropSchema",
        Statement::CreateType { .. } => "CreateType",
        Statement::CreateCast { .. } => "CreateCast",
        Statement::DropCast { .. } => "DropCast",
        Statement::CreateAccessMethod { .. } => "CreateAccessMethod",
        Statement::AlterType { .. } => "AlterType",
        Statement::DropType { .. } => "DropType",
        Statement::CreateDomain { .. } => "CreateDomain",
        Statement::AlterDomain { .. } => "AlterDomain",
        Statement::DropDomain { .. } => "DropDomain",
        Statement::AlterTable { .. } => "AlterTable",
        Statement::Comment { .. } => "Comment",
        Statement::Insert { .. } => "Insert",
        Statement::Merge { .. } => "Merge",
        Statement::CreateTableAs { .. } => "CreateTableAs",
        Statement::Truncate { .. } => "Truncate",
        Statement::Vacuum(_) => "Vacuum",
        Statement::Listen { .. } => "Listen",
        Statement::Notify { .. } => "Notify",
        Statement::Unlisten { .. } => "Unlisten",
        Statement::Query(_) => "Query",
        Statement::Begin { .. } => "Begin",
        Statement::Commit { .. } => "Commit",
        Statement::Rollback { .. } => "Rollback",
        Statement::Update { .. } => "Update",
        Statement::Delete { .. } => "Delete",
        Statement::Copy(copy) => copy_shape(copy),
        Statement::Discard { .. } => "Discard",
        Statement::Savepoint { .. } => "Savepoint",
        Statement::RollbackToSavepoint { .. } => "RollbackToSavepoint",
        Statement::ReleaseSavepoint { .. } => "ReleaseSavepoint",
        Statement::DeclareCursor { .. } => "DeclareCursor",
        Statement::FetchCursor { .. } => "FetchCursor",
        Statement::CloseCursor { .. } => "CloseCursor",
        Statement::PrepareStatement { .. } => "PrepareStatement",
        Statement::ExecuteStatement { .. } => "ExecuteStatement",
        Statement::Deallocate { .. } => "Deallocate",
        Statement::LockTable { .. } => "LockTable",
        Statement::Cluster(_) => "Cluster",
        Statement::Explain { .. } => "Explain",
        Statement::Utility(utility) => match utility {
            crabka_pgparser::ast::UtilityStatement::Analyze(_) => "Analyze",
            crabka_pgparser::ast::UtilityStatement::Reindex(_) => "Reindex",
            crabka_pgparser::ast::UtilityStatement::Checkpoint => "Checkpoint",
            crabka_pgparser::ast::UtilityStatement::Load { .. } => "Load",
            crabka_pgparser::ast::UtilityStatement::SecurityLabel { .. } => "SecurityLabel",
            crabka_pgparser::ast::UtilityStatement::CreateTablespace { .. } => "CreateTablespace",
            crabka_pgparser::ast::UtilityStatement::DropTablespace { .. } => "DropTablespace",
            crabka_pgparser::ast::UtilityStatement::AlterTablespace { .. } => "AlterTablespace",
            crabka_pgparser::ast::UtilityStatement::CreateOperatorClass { .. } => {
                "CreateOperatorClass"
            }
            crabka_pgparser::ast::UtilityStatement::CreateOperatorFamily { .. } => {
                "CreateOperatorFamily"
            }
            crabka_pgparser::ast::UtilityStatement::AlterOperatorObject { .. } => {
                "AlterOperatorObject"
            }
            crabka_pgparser::ast::UtilityStatement::DropOperatorObject { .. } => {
                "DropOperatorObject"
            }
            crabka_pgparser::ast::UtilityStatement::CreateOperator(_) => "CreateOperator",
            crabka_pgparser::ast::UtilityStatement::DropOperator { .. } => "DropOperator",
            crabka_pgparser::ast::UtilityStatement::AlterSystem { .. } => "AlterSystem",
            crabka_pgparser::ast::UtilityStatement::SetConstraints { .. } => "SetConstraints",
            crabka_pgparser::ast::UtilityStatement::SetSessionAuthorization { .. } => {
                "SetSessionAuthorization"
            }
            crabka_pgparser::ast::UtilityStatement::TextSearch(ddl) => match ddl {
                crabka_pgparser::ast::TextSearchDdl::Create { kind, .. } => match kind {
                    crabka_pgparser::ast::TextSearchObjectKind::Configuration => {
                        "CreateTextSearchConfiguration"
                    }
                    crabka_pgparser::ast::TextSearchObjectKind::Dictionary => {
                        "CreateTextSearchDictionary"
                    }
                },
                crabka_pgparser::ast::TextSearchDdl::Alter { kind, .. } => match kind {
                    crabka_pgparser::ast::TextSearchObjectKind::Configuration => {
                        "AlterTextSearchConfiguration"
                    }
                    crabka_pgparser::ast::TextSearchObjectKind::Dictionary => {
                        "AlterTextSearchDictionary"
                    }
                },
                crabka_pgparser::ast::TextSearchDdl::Drop { kind, .. } => match kind {
                    crabka_pgparser::ast::TextSearchObjectKind::Configuration => {
                        "DropTextSearchConfiguration"
                    }
                    crabka_pgparser::ast::TextSearchObjectKind::Dictionary => {
                        "DropTextSearchDictionary"
                    }
                },
            },
        },
        Statement::Set {
            local: false,
            name,
            value: crabka_pgparser::ast::SetValue::Value(value),
        } if name == "__set_transaction" && value.as_slice() == ["read committed"] => {
            "SetTransaction"
        }
        Statement::Set { .. } => "Set",
        Statement::Show { .. } => "Show",
        Statement::Reset { .. } => "Reset",
        Statement::CreateRole { .. } => "CreateRole",
        Statement::AlterRole { .. } => "AlterRole",
        Statement::AlterLargeObject { .. } => "AlterLargeObject",
        Statement::DropRole { .. } => "DropRole",
        Statement::GrantTablePrivileges { .. } => "GrantTablePrivileges",
        Statement::GrantLargeObjectPrivileges { .. } => "GrantLargeObjectPrivileges",
        Statement::GrantSchemaPrivileges { .. } => "GrantSchemaPrivileges",
        Statement::RevokeTablePrivileges { .. } => "RevokeTablePrivileges",
        Statement::RevokeLargeObjectPrivileges { .. } => "RevokeLargeObjectPrivileges",
        Statement::RevokeSchemaPrivileges { .. } => "RevokeSchemaPrivileges",
        Statement::AlterDefaultTablePrivileges { .. } => "AlterDefaultTablePrivileges",
        Statement::SetRole { .. } => "SetRole",
        Statement::CreateFdw { .. } => "CreateFdw",
        Statement::AlterFdw { .. } => "AlterFdw",
        Statement::DropFdw { .. } => "DropFdw",
        Statement::CreateServer { .. } => "CreateServer",
        Statement::AlterServer { .. } => "AlterServer",
        Statement::DropServer { .. } => "DropServer",
        Statement::CreateUserMapping { .. } => "CreateUserMapping",
        Statement::AlterUserMapping { .. } => "AlterUserMapping",
        Statement::DropUserMapping { .. } => "DropUserMapping",
        Statement::CreateForeignTable { .. } => "CreateForeignTable",
        Statement::DropForeignTable { .. } => "DropForeignTable",
        Statement::ImportForeignSchema { .. } => "ImportForeignSchema",
    }
}

/// Classify a `COPY` by the two things that decide how a client must drive it:
/// which way the rows move, and whether the far endpoint is the client (`STDIN`
/// / `STDOUT`, needing the copy subprotocol) or a server-side file. The
/// parenthesized-query source is called out separately because only `COPY … TO`
/// can spell it.
fn copy_shape(copy: &crabka_pgparser::ast::CopyStmt) -> &'static str {
    use crabka_pgparser::ast::{CopyDestination, CopyDirection, CopySource, CopyTarget};

    match (&copy.direction, &copy.target) {
        (CopyDirection::From(CopySource::Stdin), _) => "CopyFromStdin",
        (CopyDirection::From(CopySource::File(_)), _) => "CopyFromFile",
        (CopyDirection::To(CopyDestination::Stdout), CopyTarget::Table { .. }) => "CopyToStdout",
        (CopyDirection::To(CopyDestination::Stdout), CopyTarget::Query(_)) => "CopyQueryToStdout",
        (CopyDirection::To(CopyDestination::File(_)), CopyTarget::Table { .. }) => "CopyToFile",
        (CopyDirection::To(CopyDestination::File(_)), CopyTarget::Query(_)) => "CopyQueryToFile",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn report_contains_each_matrix_command_and_uses_the_stable_format() {
        let report = parser_command_report().expect("all parser command probes must parse");

        assert!(report.format_version == PARSER_COMMAND_REPORT_FORMAT_VERSION);
        assert!(
            report.commands.len() == 174,
            "all resolved command rows need probes"
        );
        assert!(report.commands.windows(2).all(|pair| pair[0] < pair[1]));
        for spec in crabka_pgparser::ast::NON_GOAL_REFUSALS {
            assert!(
                report
                    .commands
                    .iter()
                    .any(|name| name == spec.command.command_name())
            );
        }
    }

    #[test]
    fn parser_registry_distinguishes_aliases_that_share_ast_shapes() {
        use crabka_pgparser::{command::CommandIdentity, parse_with_command_identities};

        for (sql, identity) in [
            ("BEGIN", CommandIdentity::Begin),
            ("START TRANSACTION", CommandIdentity::StartTransaction),
            ("COMMIT", CommandIdentity::Commit),
            ("END", CommandIdentity::End),
            ("CREATE ROLE r", CommandIdentity::CreateRole),
            ("CREATE USER u", CommandIdentity::CreateUser),
        ] {
            let parsed = parse_with_command_identities(sql).expect(sql);
            assert!(parsed[0].1 == identity);
        }
    }

    #[test]
    fn copy_shapes_name_their_direction_endpoint_and_source() {
        for (sql, shape) in [
            ("COPY t FROM STDIN", "CopyFromStdin"),
            (
                "COPY t (a, b) FROM STDIN WITH (FORMAT csv)",
                "CopyFromStdin",
            ),
            ("COPY t FROM '/tmp/t.csv'", "CopyFromFile"),
            ("COPY t TO STDOUT", "CopyToStdout"),
            ("COPY t TO '/tmp/t.csv'", "CopyToFile"),
            ("COPY (SELECT 1) TO STDOUT", "CopyQueryToStdout"),
            ("COPY (SELECT 1) TO '/tmp/t.csv'", "CopyQueryToFile"),
        ] {
            let parsed = parse(sql).expect(sql);
            assert!(parsed.len() == 1);
            assert!(statement_shape(&parsed[0]) == shape, "{sql}");
        }
    }

    #[test]
    fn report_serializes_as_a_json_object() {
        let report = parser_command_report().expect("all parser command probes must parse");
        let json = serde_json::to_value(report).expect("report must serialize");

        assert!(json["format_version"] == PARSER_COMMAND_REPORT_FORMAT_VERSION);
        assert!(json["commands"][0] == "ABORT");
        assert!(json["probes"].as_array().map(Vec::len) == Some(174));
        let refusal = json["probes"]
            .as_array()
            .expect("probe array")
            .iter()
            .find(|probe| probe["command"] == "CREATE DATABASE")
            .expect("CREATE DATABASE probe");
        assert!(refusal["behavior"] == "refuse");
        assert!(refusal["sqlstate"] == "0A000");
    }
}
