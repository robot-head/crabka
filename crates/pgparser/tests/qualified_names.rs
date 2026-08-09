//! One policy for a dotted relation name, across every statement that names a
//! relation.
//!
//! There used to be four policies. A hardcoded schema whitelist raised `3F000`
//! at parse time. A `FROM` clause kept `s.t` unchanged. A third path also kept
//! it. A fourth path discarded the qualifier without a message.
//!
//! These tests pin the one policy that replaced the four. The parser carries
//! the qualifier and decides nothing.

use assert2::assert;
use crabka_pgparser::{
    ast::{RelationRef, Statement, TableExpr},
    parse,
};

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(statements.len() == 1);
    statements.remove(0)
}

/// The relation the statement names, for the forms that name exactly one.
fn named_relation(sql: &str) -> RelationRef {
    match one(sql) {
        Statement::CreateTable { name, .. }
        | Statement::CreateTableAs { name, .. }
        | Statement::CreateView { name, .. }
        | Statement::DropView { name, .. }
        | Statement::DropIndex { name, .. }
        | Statement::CreateType { name, .. }
        | Statement::CreateDomain { name, .. }
        | Statement::CreateForeignTable { name, .. }
        | Statement::DropForeignTable { name, .. } => name,
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. }
        | Statement::AlterTable { table, .. }
        | Statement::CreateIndex { table, .. }
        | Statement::GrantTablePrivileges { table, .. }
        | Statement::RevokeTablePrivileges { table, .. } => table,
        Statement::DropTable { mut names, .. }
        | Statement::Truncate { mut names, .. }
        | Statement::DropType { mut names, .. }
        | Statement::DropDomain { mut names, .. }
        | Statement::LockTable {
            tables: mut names, ..
        } => names.remove(0),
        Statement::Query(query) => match query.body {
            crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(
                select,
            )) => match select.from.into_iter().next() {
                Some(TableExpr::Table { name, .. }) => name,
                other => panic!("{sql}: expected a base table in FROM, got {other:?}"),
            },
            other => panic!("{sql}: expected a SELECT, got {other:?}"),
        },
        other => panic!("{sql}: no single relation name on {other:?}"),
    }
}

/// Every spelling `{}` can take, one per statement form that names a relation.
const FORMS: &[&str] = &[
    "SELECT * FROM {}",
    "SELECT * FROM {} AS q",
    "TABLE {}",
    "INSERT INTO {} VALUES (1)",
    "UPDATE {} SET x = 1",
    "DELETE FROM {}",
    "MERGE INTO {} USING src ON true WHEN MATCHED THEN DO NOTHING",
    "CREATE TABLE {} (x int)",
    "CREATE TABLE {} AS SELECT 1",
    "CREATE VIEW {} AS SELECT 1",
    "CREATE INDEX ON {} (x)",
    "CREATE FOREIGN TABLE {} (x int) SERVER s OPTIONS (a 'b')",
    "ALTER TABLE {} ADD COLUMN y int",
    "DROP TABLE {}",
    "DROP VIEW {}",
    "DROP INDEX {}",
    "DROP FOREIGN TABLE {}",
    "TRUNCATE {}",
    "LOCK TABLE {}",
    "GRANT SELECT ON TABLE {} TO r",
    "REVOKE SELECT ON TABLE {} FROM r",
];

/// A qualifier survives into the AST from every statement form, whatever the
/// schema is called. The parser strips nothing and refuses nothing here.
#[test]
fn every_statement_form_carries_the_schema_it_was_written_with() {
    for form in FORMS {
        for schema in [
            "s1",
            "public",
            "pg_temp",
            "pg_catalog",
            "information_schema",
        ] {
            let sql = form.replace("{}", &format!("{schema}.t"));
            assert!(
                named_relation(&sql) == RelationRef::qualified(schema, "t"),
                "{sql}"
            );
        }
        let sql = form.replace("{}", "t");
        assert!(named_relation(&sql) == RelationRef::bare("t"), "{sql}");
    }
}

/// The lexer folds an unquoted identifier and keeps a quoted one. So a
/// `RelationRef` built from its tokens already renders the form `PostgreSQL`
/// names in `relation "s.t" does not exist`.
#[test]
fn a_relation_ref_is_case_folded_and_renders_dotted() {
    assert!(named_relation("SELECT * FROM S.T") == RelationRef::qualified("s", "t"));
    assert!(named_relation("SELECT * FROM S.T").to_string() == "s.t");
    assert!(named_relation("SELECT * FROM t").to_string() == "t");
    // A quoted name is one name that happens to hold a dot, not a qualifier.
    assert!(named_relation("SELECT * FROM \"a.b\"") == RelationRef::bare("a.b"));
    assert!(named_relation("SELECT * FROM \"S\".\"T\"") == RelationRef::qualified("S", "T"));
}

/// The parser does not check the schema a statement names at all. It has no
/// catalog. `PostgreSQL` reports a missing schema differently for each
/// statement, and only the executor can decide that.
#[test]
fn a_nonexistent_schema_is_not_a_parse_error() {
    for form in FORMS {
        let sql = form.replace("{}", "nope.t");
        assert!(parse(&sql).is_ok(), "{sql}");
    }
}

/// Three parts can only mean one thing where there is one database.
#[test]
fn a_three_part_name_is_the_cross_database_refusal() {
    for form in FORMS {
        let sql = form.replace("{}", "db.s.t");
        let error = parse(&sql).expect_err(&sql);
        assert!(error.sqlstate() == "0A000", "{sql}");
        assert!(
            error.message == "cross-database references are not implemented: \"db.s.t\"",
            "{sql}"
        );
    }
}

/// A qualified name in `FROM` position followed by `(` is still a function
/// call, not a relation.
#[test]
fn a_qualified_from_item_with_arguments_is_a_function_call() {
    let Statement::Query(query) = one("SELECT * FROM pg_catalog.generate_series(1, 3)") else {
        panic!("expected a query");
    };
    let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(select)) =
        query.body
    else {
        panic!("expected a SELECT");
    };
    let [TableExpr::Function { functions, .. }] = select.from.as_slice() else {
        panic!("expected a function item, got {:?}", select.from);
    };
    assert!(functions[0].name == "pg_catalog.generate_series");
}

/// `CREATE SEQUENCE` and `DROP SEQUENCE` share the index/table variants. The
/// sentinel that tags them sits in the relation's own name, so the qualifier
/// stays where the resolver can see it.
#[test]
fn the_sequence_spelling_keeps_its_qualifier() {
    let Statement::CreateIndex { name, table, .. } = one("CREATE SEQUENCE s1.seq") else {
        panic!("expected the CREATE SEQUENCE spelling");
    };
    assert!(name == Some(RelationRef::qualified("s1", "seq")));
    assert!(table == RelationRef::bare("__crabka_sequence__"));

    let Statement::DropTable { names, .. } = one("DROP SEQUENCE s1.seq, seq2") else {
        panic!("expected the DROP SEQUENCE spelling");
    };
    assert!(
        names
            == vec![
                RelationRef::qualified("s1", "__crabka_sequence__:seq"),
                RelationRef::bare("__crabka_sequence__:seq2"),
            ]
    );
}
