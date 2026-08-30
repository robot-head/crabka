//! The grammar `PostgreSQL` 17 and 18 added around `CONSTRAINT`: the
//! table-level `[CONSTRAINT n] NOT NULL <column>`, and `ALTER TABLE … ALTER
//! CONSTRAINT`.
//!
//! `CONSTRAINT n NOT NULL` is the one place the table-level and column-level
//! constraint grammars spell the same prefix, so which one a statement wrote is
//! settled by whether a column name follows. Every refusal asserted here comes
//! from a live `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgparser::{
    ParseError,
    ast::{
        AlterConstraintSpec, AlterTableAction, ColumnConstraintKind, Statement, TableConstraint,
        TableConstraintKind,
    },
    parse,
};

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(statements.len() == 1);
    statements.remove(0)
}

fn err(sql: &str) -> ParseError {
    parse(sql).expect_err(sql)
}

// The table-level constraint list of a `CREATE TABLE`.
fn table_constraints(sql: &str) -> Vec<TableConstraint> {
    match one(sql) {
        Statement::CreateTable { constraints, .. } => constraints,
        other => panic!("{sql} parsed as {other:?}"),
    }
}

fn alter_actions(sql: &str) -> Vec<AlterTableAction> {
    match one(sql) {
        Statement::AlterTable { actions, .. } => actions,
        other => panic!("{sql} parsed as {other:?}"),
    }
}

fn not_null(name: Option<&str>, column: &str, no_inherit: bool) -> TableConstraint {
    TableConstraint {
        name: name.map(ToString::to_string),
        kind: TableConstraintKind::NotNull {
            column: column.into(),
            no_inherit,
        },
        attributes: crabka_pgparser::ast::ConstraintAttributes {
            no_inherit,
            ..Default::default()
        },
    }
}

// Both spellings of the table-level not-null, in every position the element
// list admits, and with the attribute tail that belongs to it.
#[test]
fn the_table_level_not_null_carries_its_column_name_and_label() {
    let cases: &[(&str, TableConstraint)] = &[
        (
            "CREATE TABLE t (a int, NOT NULL a)",
            not_null(None, "a", false),
        ),
        (
            "CREATE TABLE t (a int, CONSTRAINT c NOT NULL a)",
            not_null(Some("c"), "a", false),
        ),
        (
            "CREATE TABLE t (CONSTRAINT c NOT NULL a, a int)",
            not_null(Some("c"), "a", false),
        ),
        (
            "CREATE TABLE t (a int, NOT NULL a NO INHERIT)",
            not_null(None, "a", true),
        ),
        (
            "CREATE TABLE t (a int, CONSTRAINT c NOT NULL a NO INHERIT)",
            not_null(Some("c"), "a", true),
        ),
    ];
    for (sql, expected) in cases {
        assert!(table_constraints(sql) == vec![expected.clone()], "{sql}");
    }
}

// `NOT VALID` reaches the attributes rather than the kind, because it is the
// one attribute every table constraint shares.
#[test]
fn not_valid_on_a_table_level_not_null_reaches_the_attributes() {
    let actions = alter_actions("ALTER TABLE t ADD CONSTRAINT nn NOT NULL a NOT VALID");
    let [AlterTableAction::AddConstraint(constraint)] = actions.as_slice() else {
        panic!("expected one ADD CONSTRAINT, got {actions:?}");
    };
    assert!(constraint.attributes.not_valid);
    assert!(
        constraint.kind
            == TableConstraintKind::NotNull {
                column: "a".into(),
                no_inherit: false,
            }
    );
}

// `CONSTRAINT c NOT NULL` inside a column definition stays a column constraint:
// a table-level one has to name its column, and a column definition cannot
// begin with the reserved `CONSTRAINT`.
#[test]
fn a_column_level_not_null_is_not_mistaken_for_the_table_level_one() {
    let Statement::CreateTable {
        columns,
        constraints,
        ..
    } = one("CREATE TABLE t (a int CONSTRAINT c NOT NULL, CONSTRAINT d NOT NULL a)")
    else {
        panic!("expected CREATE TABLE");
    };
    assert!(constraints == vec![not_null(Some("d"), "a", false)]);
    assert!(columns.len() == 1);
    assert!(columns[0].constraints.len() == 1);
    assert!(columns[0].constraints[0].name.as_deref() == Some("c"));
    assert!(columns[0].constraints[0].kind == ColumnConstraintKind::NotNull);
}

// Each `ALTER CONSTRAINT` clause writes exactly the properties it names and
// leaves the rest for the executor to keep.
#[test]
fn alter_constraint_records_only_the_properties_that_were_written() {
    let cases: &[(&str, AlterConstraintSpec)] = &[
        (
            "DEFERRABLE",
            AlterConstraintSpec {
                deferrability: Some((true, false)),
                enforced: None,
                inherit: None,
            },
        ),
        (
            "DEFERRABLE INITIALLY DEFERRED",
            AlterConstraintSpec {
                deferrability: Some((true, true)),
                enforced: None,
                inherit: None,
            },
        ),
        (
            "INITIALLY DEFERRED",
            AlterConstraintSpec {
                deferrability: Some((true, true)),
                enforced: None,
                inherit: None,
            },
        ),
        (
            "NOT DEFERRABLE",
            AlterConstraintSpec {
                deferrability: Some((false, false)),
                enforced: None,
                inherit: None,
            },
        ),
        (
            "ENFORCED",
            AlterConstraintSpec {
                deferrability: None,
                enforced: Some(true),
                inherit: None,
            },
        ),
        (
            "NOT ENFORCED",
            AlterConstraintSpec {
                deferrability: None,
                enforced: Some(false),
                inherit: None,
            },
        ),
        (
            "NO INHERIT",
            AlterConstraintSpec {
                deferrability: None,
                enforced: None,
                inherit: Some(false),
            },
        ),
        (
            "INHERIT",
            AlterConstraintSpec {
                deferrability: None,
                enforced: None,
                inherit: Some(true),
            },
        ),
        (
            "NOT ENFORCED NOT DEFERRABLE",
            AlterConstraintSpec {
                deferrability: Some((false, false)),
                enforced: Some(false),
                inherit: None,
            },
        ),
    ];
    for (tail, spec) in cases {
        let sql = format!("ALTER TABLE t ALTER CONSTRAINT c {tail}");
        assert!(
            alter_actions(&sql)
                == vec![AlterTableAction::AlterConstraint {
                    name: "c".into(),
                    spec: *spec,
                }],
            "{sql}"
        );
    }
}

// The three refusals PostgreSQL words itself in the grammar, rather than
// reporting the token it stopped on.
#[test]
fn the_grammar_refuses_the_attribute_combinations_postgres_refuses() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "ALTER TABLE t ALTER CONSTRAINT c NOT VALID",
            "0A000",
            "constraints cannot be altered to be NOT VALID",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT c DEFERRABLE NOT VALID",
            "0A000",
            "constraints cannot be altered to be NOT VALID",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT c ENFORCED NOT ENFORCED",
            "42601",
            "conflicting constraint properties",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED ENFORCED",
            "42601",
            "conflicting constraint properties",
        ),
        (
            "ALTER TABLE t ALTER CONSTRAINT c NOT DEFERRABLE INITIALLY DEFERRED",
            "42601",
            "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
        ),
    ];
    for (sql, sqlstate, message) in cases {
        let error = err(sql);
        assert!(error.message == *message, "{sql}");
        assert!(error.sqlstate() == *sqlstate, "{sql}");
    }
}

// `ENFORCED NOT ENFORCED` is a conflict wherever the attribute list appears,
// not only after `ALTER CONSTRAINT`.
#[test]
fn conflicting_enforceability_is_refused_on_a_written_constraint_too() {
    assert!(
        err("CREATE TABLE t (a int, FOREIGN KEY (a) REFERENCES p (id) ENFORCED NOT ENFORCED)")
            .message
            == "conflicting constraint properties"
    );
}
