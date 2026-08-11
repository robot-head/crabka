// Every command tag PostgreSQL knows, with the two flags event triggers ask
// about. Generated; do not edit by hand.
//
// Source: src/include/tcop/cmdtaglist.h of PostgreSQL 18.4, which holds
// one PG_CMDTAG(symbol, name, event_trigger_ok, table_rewrite_ok, rowcount)
// line per tag. rowcount is dropped: nothing here reports a row count.
// CMDTAG_UNKNOWN ("???") is dropped too, because a lookup miss already means
// "unknown tag" and keeping the sentinel would make '???' a tag that CREATE
// EVENT TRIGGER accepts by name.
//
// Regenerate with:
//
//     scripts/generate-command-tags.sh \
//         target/pg-regress-postgresql-18.4/source/src/include/tcop/cmdtaglist.h \
//         crates/pgexec/src/event_command_tags.rs
//
// trigger.rs include!s this file, so it declares no module of its own and
// CommandTag is the type that module defines. Ordinary comments rather than
// doc comments, because an included file is spliced into the middle of a
// module, where an inner doc comment does not parse.

// The tag table, in the header's order, which is alphabetical by name.
const COMMAND_TAGS: &[CommandTag] = &[
    CommandTag {
        name: "ALTER ACCESS METHOD",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER AGGREGATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER CAST",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER COLLATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER CONSTRAINT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER CONVERSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER DATABASE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER DEFAULT PRIVILEGES",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER DOMAIN",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER EVENT TRIGGER",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER EXTENSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER FOREIGN DATA WRAPPER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER FOREIGN TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER FUNCTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER INDEX",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER LANGUAGE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER LARGE OBJECT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER MATERIALIZED VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: true,
    },
    CommandTag {
        name: "ALTER OPERATOR",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER OPERATOR CLASS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER OPERATOR FAMILY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER POLICY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER PROCEDURE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER PUBLICATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER ROLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER ROUTINE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER RULE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER SCHEMA",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER SEQUENCE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER SERVER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER STATISTICS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER SUBSCRIPTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER SYSTEM",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: true,
    },
    CommandTag {
        name: "ALTER TABLESPACE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TEXT SEARCH CONFIGURATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TEXT SEARCH DICTIONARY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TEXT SEARCH PARSER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TEXT SEARCH TEMPLATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TRANSFORM",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TRIGGER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER TYPE",
        event_trigger_ok: true,
        table_rewrite_ok: true,
    },
    CommandTag {
        name: "ALTER USER MAPPING",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ALTER VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ANALYZE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "BEGIN",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CALL",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CHECKPOINT",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CLOSE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CLOSE CURSOR",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CLOSE CURSOR ALL",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CLUSTER",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "COMMENT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "COMMIT",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "COMMIT PREPARED",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "COPY",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "COPY FROM",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE ACCESS METHOD",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE AGGREGATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE CAST",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE COLLATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE CONSTRAINT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE CONVERSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE DATABASE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE DOMAIN",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE EVENT TRIGGER",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE EXTENSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE FOREIGN DATA WRAPPER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE FOREIGN TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE FUNCTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE INDEX",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE LANGUAGE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE MATERIALIZED VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE OPERATOR",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE OPERATOR CLASS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE OPERATOR FAMILY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE POLICY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE PROCEDURE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE PUBLICATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE ROLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE ROUTINE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE RULE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE SCHEMA",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE SEQUENCE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE SERVER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE STATISTICS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE SUBSCRIPTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TABLE AS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TABLESPACE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TEXT SEARCH CONFIGURATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TEXT SEARCH DICTIONARY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TEXT SEARCH PARSER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TEXT SEARCH TEMPLATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TRANSFORM",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TRIGGER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE TYPE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE USER MAPPING",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "CREATE VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DEALLOCATE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DEALLOCATE ALL",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DECLARE CURSOR",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DELETE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DISCARD",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DISCARD ALL",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DISCARD PLANS",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DISCARD SEQUENCES",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DISCARD TEMP",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DO",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP ACCESS METHOD",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP AGGREGATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP CAST",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP COLLATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP CONSTRAINT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP CONVERSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP DATABASE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP DOMAIN",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP EVENT TRIGGER",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP EXTENSION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP FOREIGN DATA WRAPPER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP FOREIGN TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP FUNCTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP INDEX",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP LANGUAGE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP MATERIALIZED VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP OPERATOR",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP OPERATOR CLASS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP OPERATOR FAMILY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP OWNED",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP POLICY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP PROCEDURE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP PUBLICATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP ROLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP ROUTINE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP RULE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP SCHEMA",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP SEQUENCE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP SERVER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP STATISTICS",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP SUBSCRIPTION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TABLE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TABLESPACE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TEXT SEARCH CONFIGURATION",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TEXT SEARCH DICTIONARY",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TEXT SEARCH PARSER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TEXT SEARCH TEMPLATE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TRANSFORM",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TRIGGER",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP TYPE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP USER MAPPING",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "DROP VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "EXECUTE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "EXPLAIN",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "FETCH",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "GRANT",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "GRANT ROLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "IMPORT FOREIGN SCHEMA",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "INSERT",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "LISTEN",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "LOAD",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "LOCK TABLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "LOGIN",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "MERGE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "MOVE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "NOTIFY",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "PREPARE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "PREPARE TRANSACTION",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "REASSIGN OWNED",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "REFRESH MATERIALIZED VIEW",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "REINDEX",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "RELEASE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "RESET",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "REVOKE",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "REVOKE ROLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ROLLBACK",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "ROLLBACK PREPARED",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SAVEPOINT",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SECURITY LABEL",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT FOR KEY SHARE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT FOR NO KEY UPDATE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT FOR SHARE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT FOR UPDATE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SELECT INTO",
        event_trigger_ok: true,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SET",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SET CONSTRAINTS",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "SHOW",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "START TRANSACTION",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "TRUNCATE TABLE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "UNLISTEN",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "UPDATE",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
    CommandTag {
        name: "VACUUM",
        event_trigger_ok: false,
        table_rewrite_ok: false,
    },
];
