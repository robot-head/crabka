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
        #[source]
        source: ParseError,
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
}

const COMMAND_PROBES: &[CommandProbe] = &[
    CommandProbe {
        command: "ALTER DATABASE",
        sql: "ALTER DATABASE postgres RENAME TO other",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "CREATE DATABASE",
        sql: "CREATE DATABASE other",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "DROP DATABASE",
        sql: "DROP DATABASE other",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "ALTER EXTENSION",
        sql: "ALTER EXTENSION plpgsql UPDATE",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "DROP EXTENSION",
        sql: "DROP EXTENSION plpgsql",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "PREPARE TRANSACTION",
        sql: "PREPARE TRANSACTION 'xid-1'",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "COMMIT PREPARED",
        sql: "COMMIT PREPARED 'xid-1'",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "ROLLBACK PREPARED",
        sql: "ROLLBACK PREPARED 'xid-1'",
        expected_statement: "CompatibilityRefusal",
    },
    CommandProbe {
        command: "CREATE TABLE",
        sql: "CREATE TABLE parser_commands_probe (id int4)",
        expected_statement: "CreateTable",
    },
    CommandProbe {
        command: "CREATE VIEW",
        sql: "CREATE VIEW parser_commands_view AS SELECT 1",
        expected_statement: "CreateView",
    },
    CommandProbe {
        command: "CREATE INDEX",
        sql: "CREATE INDEX parser_commands_probe_index ON parser_commands_probe (id)",
        expected_statement: "CreateIndex",
    },
    CommandProbe {
        command: "CREATE SEQUENCE",
        sql: "CREATE SEQUENCE parser_commands_probe_sequence",
        expected_statement: "CreateSequence",
    },
    CommandProbe {
        command: "DROP TABLE",
        sql: "DROP TABLE parser_commands_probe",
        expected_statement: "DropTable",
    },
    CommandProbe {
        command: "DROP VIEW",
        sql: "DROP VIEW parser_commands_view",
        expected_statement: "DropView",
    },
    CommandProbe {
        command: "DROP INDEX",
        sql: "DROP INDEX IF EXISTS parser_commands_probe_index",
        expected_statement: "DropIndex",
    },
    CommandProbe {
        command: "DROP SEQUENCE",
        sql: "DROP SEQUENCE parser_commands_probe_sequence",
        expected_statement: "DropSequence",
    },
    CommandProbe {
        command: "ALTER TABLE",
        sql: "ALTER TABLE parser_commands_probe RENAME TO parser_commands_renamed_probe",
        expected_statement: "AlterTableRename",
    },
    CommandProbe {
        command: "INSERT",
        sql: "INSERT INTO parser_commands_probe VALUES (1)",
        expected_statement: "Insert",
    },
    CommandProbe {
        command: "TRUNCATE",
        sql: "TRUNCATE parser_commands_probe",
        expected_statement: "Truncate",
    },
    CommandProbe {
        command: "VACUUM",
        sql: "VACUUM ANALYZE parser_commands_probe",
        expected_statement: "Vacuum",
    },
    CommandProbe {
        command: "SELECT",
        sql: "SELECT 1",
        expected_statement: "Query",
    },
    CommandProbe {
        command: "VALUES",
        sql: "VALUES (1)",
        expected_statement: "Query",
    },
    CommandProbe {
        command: "BEGIN",
        sql: "BEGIN",
        expected_statement: "Begin",
    },
    CommandProbe {
        command: "START TRANSACTION",
        sql: "START TRANSACTION",
        expected_statement: "Begin",
    },
    CommandProbe {
        command: "COMMIT",
        sql: "COMMIT",
        expected_statement: "Commit",
    },
    CommandProbe {
        command: "END",
        sql: "END",
        expected_statement: "Commit",
    },
    CommandProbe {
        command: "ROLLBACK",
        sql: "ROLLBACK",
        expected_statement: "Rollback",
    },
    CommandProbe {
        command: "ABORT",
        sql: "ABORT",
        expected_statement: "Rollback",
    },
    CommandProbe {
        command: "UPDATE",
        sql: "UPDATE parser_commands_probe SET id = 1",
        expected_statement: "Update",
    },
    CommandProbe {
        command: "DELETE",
        sql: "DELETE FROM parser_commands_probe",
        expected_statement: "Delete",
    },
    CommandProbe {
        command: "SET",
        sql: "SET extra_float_digits TO 2",
        expected_statement: "Set",
    },
    CommandProbe {
        command: "SET TRANSACTION",
        sql: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        expected_statement: "SetTransaction",
    },
    CommandProbe {
        command: "SHOW",
        sql: "SHOW extra_float_digits",
        expected_statement: "Show",
    },
    CommandProbe {
        command: "RESET",
        sql: "RESET extra_float_digits",
        expected_statement: "Reset",
    },
    CommandProbe {
        command: "DISCARD",
        sql: "DISCARD ALL",
        expected_statement: "DiscardAll",
    },
    CommandProbe {
        command: "COPY",
        sql: "COPY parser_commands_probe FROM STDIN",
        expected_statement: "CopyFromStdin",
    },
    CommandProbe {
        command: "CREATE FOREIGN DATA WRAPPER",
        sql: "CREATE FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "CreateFdw",
    },
    CommandProbe {
        command: "DROP FOREIGN DATA WRAPPER",
        sql: "DROP FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "DropFdw",
    },
    CommandProbe {
        command: "CREATE SERVER",
        sql: "CREATE SERVER parser_commands_server FOREIGN DATA WRAPPER parser_commands_wrapper",
        expected_statement: "CreateServer",
    },
    CommandProbe {
        command: "ALTER SERVER",
        sql: "ALTER SERVER parser_commands_server OPTIONS (host 'localhost')",
        expected_statement: "AlterServer",
    },
    CommandProbe {
        command: "DROP SERVER",
        sql: "DROP SERVER parser_commands_server",
        expected_statement: "DropServer",
    },
    CommandProbe {
        command: "CREATE USER MAPPING",
        sql: "CREATE USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        expected_statement: "CreateUserMapping",
    },
    CommandProbe {
        command: "ALTER USER MAPPING",
        sql: "ALTER USER MAPPING FOR PUBLIC SERVER parser_commands_server OPTIONS (username 'crab')",
        expected_statement: "AlterUserMapping",
    },
    CommandProbe {
        command: "DROP USER MAPPING",
        sql: "DROP USER MAPPING FOR PUBLIC SERVER parser_commands_server",
        expected_statement: "DropUserMapping",
    },
    CommandProbe {
        command: "CREATE FOREIGN TABLE",
        sql: "CREATE FOREIGN TABLE parser_commands_foreign (id int4) SERVER parser_commands_server",
        expected_statement: "CreateForeignTable",
    },
    CommandProbe {
        command: "DROP FOREIGN TABLE",
        sql: "DROP FOREIGN TABLE parser_commands_foreign",
        expected_statement: "DropForeignTable",
    },
    CommandProbe {
        command: "IMPORT FOREIGN SCHEMA",
        sql: "IMPORT FOREIGN SCHEMA parser_commands_schema FROM SERVER parser_commands_server",
        expected_statement: "ImportForeignSchema",
    },
    CommandProbe {
        command: "CREATE ROLE",
        sql: "CREATE ROLE parser_commands_role",
        expected_statement: "CreateRole",
    },
    CommandProbe {
        command: "CREATE USER",
        sql: "CREATE USER parser_commands_user",
        expected_statement: "CreateRole",
    },
    CommandProbe {
        command: "DROP ROLE",
        sql: "DROP ROLE parser_commands_role",
        expected_statement: "DropRole",
    },
    CommandProbe {
        command: "DROP USER",
        sql: "DROP USER parser_commands_user",
        expected_statement: "DropRole",
    },
    CommandProbe {
        command: "GRANT",
        sql: "GRANT SELECT ON TABLE parser_commands_probe TO parser_commands_role",
        expected_statement: "GrantTablePrivileges",
    },
    CommandProbe {
        command: "REVOKE",
        sql: "REVOKE SELECT ON TABLE parser_commands_probe FROM parser_commands_role",
        expected_statement: "RevokeTablePrivileges",
    },
    CommandProbe {
        command: "SET ROLE",
        sql: "SET ROLE parser_commands_role",
        expected_statement: "SetRole",
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
        source,
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
                source,
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
        Statement::CreateIndex { table, .. } if table == "__crabka_sequence__" => "CreateSequence",
        Statement::CreateIndex { .. } => "CreateIndex",
        Statement::DropIndex { .. } => "DropIndex",
        Statement::DropTable { names, .. }
            if names
                .first()
                .is_some_and(|name| name.starts_with("__crabka_sequence__:")) =>
        {
            "DropSequence"
        }
        Statement::DropTable { .. } => "DropTable",
        Statement::DropView { .. } => "DropView",
        Statement::AlterTableRename { .. } => "AlterTableRename",
        Statement::Insert { .. } => "Insert",
        Statement::Truncate { .. } => "Truncate",
        Statement::Vacuum => "Vacuum",
        Statement::Query(_) => "Query",
        Statement::Begin { .. } => "Begin",
        Statement::Commit => "Commit",
        Statement::Rollback => "Rollback",
        Statement::Update { .. } => "Update",
        Statement::Delete { .. } => "Delete",
        Statement::Set { name, .. } if name == crabka_pgparser::ast::COPY_FROM_STDIN_SENTINEL => {
            "CopyFromStdin"
        }
        Statement::Set { name, .. } if name == "__discard_all" => "DiscardAll",
        Statement::Set {
            local: false,
            name,
            value: crabka_pgparser::ast::SetValue::Value(value),
        } if name == "__set_transaction" && value == "read committed" => "SetTransaction",
        Statement::Set { .. } => "Set",
        Statement::Show { .. } => "Show",
        Statement::Reset { .. } => "Reset",
        Statement::CreateRole { .. } => "CreateRole",
        Statement::DropRole { .. } => "DropRole",
        Statement::GrantTablePrivileges { .. } => "GrantTablePrivileges",
        Statement::RevokeTablePrivileges { .. } => "RevokeTablePrivileges",
        Statement::SetRole { .. } => "SetRole",
        Statement::CreateFdw { .. } => "CreateFdw",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_contains_each_matrix_command_and_uses_the_stable_format() {
        let report = parser_command_report().expect("all parser command probes must parse");

        assert_eq!(report.format_version, PARSER_COMMAND_REPORT_FORMAT_VERSION);
        assert_eq!(
            report.commands.len(),
            94,
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
            assert_eq!(parsed[0].1, identity);
        }
    }

    #[test]
    fn report_serializes_as_a_json_object() {
        let report = parser_command_report().expect("all parser command probes must parse");
        let json = serde_json::to_value(report).expect("report must serialize");

        assert_eq!(json["format_version"], PARSER_COMMAND_REPORT_FORMAT_VERSION);
        assert_eq!(json["commands"][0], "ABORT");
        assert_eq!(json["probes"].as_array().map(Vec::len), Some(94));
        let refusal = json["probes"]
            .as_array()
            .expect("probe array")
            .iter()
            .find(|probe| probe["command"] == "CREATE DATABASE")
            .expect("CREATE DATABASE probe");
        assert_eq!(refusal["behavior"], "refuse");
        assert_eq!(refusal["sqlstate"], "0A000");
    }
}
