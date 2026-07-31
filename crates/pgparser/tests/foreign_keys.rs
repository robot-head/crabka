//! The `FOREIGN KEY` grammar: the `REFERENCES` target, `MATCH`, the referential
//! actions and their `ON DELETE SET …` column lists, the constraint
//! deferrability tail, and `SET CONSTRAINTS`.
//!
//! Every refusal asserted here was captured from a live `PostgreSQL` 18.4.

use assert2::assert;
use crabka_pgparser::{
    ParseError,
    ast::{
        ColumnConstraint, ColumnConstraintKind, ConstraintAttributes, ForeignKeyRef, MatchType,
        ReferentialAction, Statement, TableConstraint, TableConstraintKind, UtilityStatement,
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

// The single table constraint of a one-constraint `CREATE TABLE`.
fn table_constraint(sql: &str) -> TableConstraint {
    match one(sql) {
        Statement::CreateTable {
            mut constraints, ..
        } => {
            assert!(constraints.len() == 1, "{sql}");
            constraints.remove(0)
        }
        other => panic!("{sql} parsed as {other:?}"),
    }
}

// The single column constraint of the first column of a `CREATE TABLE`.
fn column_constraint(sql: &str) -> ColumnConstraint {
    match one(sql) {
        Statement::CreateTable { mut columns, .. } => {
            let mut column = columns.remove(0);
            assert!(column.constraints.len() == 1, "{sql}");
            column.constraints.remove(0)
        }
        other => panic!("{sql} parsed as {other:?}"),
    }
}

// The `ForeignKeyRef` of the table-level `FOREIGN KEY (a) REFERENCES …` form,
// where `tail` is everything after `REFERENCES p (id)`.
fn table_level_ref(tail: &str) -> ForeignKeyRef {
    let sql = format!("CREATE TABLE c (a int, b int, FOREIGN KEY (a) REFERENCES p (id) {tail})");
    match table_constraint(&sql).kind {
        TableConstraintKind::ForeignKey {
            columns,
            references,
        } => {
            assert!(columns == vec!["a".to_string()], "{sql}");
            references
        }
        other => panic!("{sql} parsed as {other:?}"),
    }
}

// The `ForeignKeyRef` of the column-level `a int REFERENCES …` form, with the
// same tail.
fn column_level_ref(tail: &str) -> ForeignKeyRef {
    let sql = format!("CREATE TABLE c (a int REFERENCES p (id) {tail}, b int)");
    match column_constraint(&sql).kind {
        ColumnConstraintKind::References(references) => references,
        other => panic!("{sql} parsed as {other:?}"),
    }
}

// A `ForeignKeyRef` onto `p (id)` with defaults for everything not given.
fn fk(
    match_type: MatchType,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
    set_columns: &[&str],
) -> ForeignKeyRef {
    ForeignKeyRef {
        table: "p".into(),
        columns: vec!["id".into()],
        match_type,
        on_delete,
        on_update,
        set_columns: set_columns.iter().map(|&c| c.into()).collect(),
    }
}

struct RefCase {
    tail: &'static str,
    expected: ForeignKeyRef,
}

// Every referential action, on both sides, in both clause orders, with every
// `MATCH` spelling — parsed identically by the column-level and table-level
// spellings of the same clause.
#[test]
fn every_reference_tail_parses_the_same_in_both_spellings() {
    use MatchType::{Full, Simple};
    use ReferentialAction::{Cascade, NoAction, Restrict, SetDefault, SetNull};

    let cases = &[
        RefCase {
            tail: "",
            expected: fk(Simple, NoAction, NoAction, &[]),
        },
        // Every action, on ON DELETE.
        RefCase {
            tail: "ON DELETE NO ACTION",
            expected: fk(Simple, NoAction, NoAction, &[]),
        },
        RefCase {
            tail: "ON DELETE RESTRICT",
            expected: fk(Simple, Restrict, NoAction, &[]),
        },
        RefCase {
            tail: "ON DELETE CASCADE",
            expected: fk(Simple, Cascade, NoAction, &[]),
        },
        RefCase {
            tail: "ON DELETE SET NULL",
            expected: fk(Simple, SetNull, NoAction, &[]),
        },
        RefCase {
            tail: "ON DELETE SET DEFAULT",
            expected: fk(Simple, SetDefault, NoAction, &[]),
        },
        // Every action, on ON UPDATE.
        RefCase {
            tail: "ON UPDATE NO ACTION",
            expected: fk(Simple, NoAction, NoAction, &[]),
        },
        RefCase {
            tail: "ON UPDATE RESTRICT",
            expected: fk(Simple, NoAction, Restrict, &[]),
        },
        RefCase {
            tail: "ON UPDATE CASCADE",
            expected: fk(Simple, NoAction, Cascade, &[]),
        },
        RefCase {
            tail: "ON UPDATE SET NULL",
            expected: fk(Simple, NoAction, SetNull, &[]),
        },
        RefCase {
            tail: "ON UPDATE SET DEFAULT",
            expected: fk(Simple, NoAction, SetDefault, &[]),
        },
        // Both clauses, in either order.
        RefCase {
            tail: "ON DELETE CASCADE ON UPDATE RESTRICT",
            expected: fk(Simple, Cascade, Restrict, &[]),
        },
        RefCase {
            tail: "ON UPDATE RESTRICT ON DELETE CASCADE",
            expected: fk(Simple, Cascade, Restrict, &[]),
        },
        // MATCH, in all three spellings, and its interaction with the actions.
        RefCase {
            tail: "MATCH SIMPLE",
            expected: fk(Simple, NoAction, NoAction, &[]),
        },
        RefCase {
            tail: "MATCH FULL",
            expected: fk(Full, NoAction, NoAction, &[]),
        },
        RefCase {
            tail: "MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL",
            expected: fk(Full, SetNull, Cascade, &[]),
        },
        // The ON DELETE SET column lists, in written order.
        RefCase {
            tail: "ON DELETE SET NULL (a)",
            expected: fk(Simple, SetNull, NoAction, &["a"]),
        },
        RefCase {
            tail: "ON DELETE SET DEFAULT (b, a)",
            expected: fk(Simple, SetDefault, NoAction, &["b", "a"]),
        },
        RefCase {
            tail: "ON UPDATE CASCADE ON DELETE SET NULL (a, b)",
            expected: fk(Simple, SetNull, Cascade, &["a", "b"]),
        },
    ];

    for case in cases {
        assert!(table_level_ref(case.tail) == case.expected, "{}", case.tail);
        assert!(
            column_level_ref(case.tail) == case.expected,
            "{}",
            case.tail
        );
    }
}

// A composite key keeps both column lists exactly as written — PostgreSQL
// pairs them positionally and names the constraint from the referencing list
// in that order, so neither may be sorted or permuted.
#[test]
fn composite_column_lists_keep_their_written_order() {
    let constraint = table_constraint(
        "CREATE TABLE c (a int, b int, FOREIGN KEY (b, a) REFERENCES p (y, x) MATCH FULL)",
    );
    assert!(
        constraint
            == TableConstraint {
                name: None,
                kind: TableConstraintKind::ForeignKey {
                    columns: vec!["b".into(), "a".into()],
                    references: ForeignKeyRef {
                        table: "p".into(),
                        columns: vec!["y".into(), "x".into()],
                        match_type: MatchType::Full,
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                        set_columns: Vec::new(),
                    },
                },
                attributes: ConstraintAttributes::default(),
            }
    );
}

// Omitting the referenced column list means the referenced table's primary
// key, which the parser records as an empty list rather than inventing one.
#[test]
fn an_omitted_referenced_column_list_stays_empty() {
    let constraint = table_constraint("CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p)");
    assert!(
        constraint.kind
            == TableConstraintKind::ForeignKey {
                columns: vec!["a".into()],
                references: ForeignKeyRef {
                    table: "p".into(),
                    columns: Vec::new(),
                    ..ForeignKeyRef::default()
                },
            }
    );
}

struct RefusalCase {
    sql: &'static str,
    sqlstate: &'static str,
    message: &'static str,
}

// The two refusals PostgreSQL raises at parse analysis, byte for byte.
#[test]
fn postgres_parse_time_refusals_are_reproduced_verbatim() {
    let cases = &[
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) MATCH PARTIAL)",
            sqlstate: "0A000",
            message: "MATCH PARTIAL not yet implemented",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH PARTIAL)",
            sqlstate: "0A000",
            message: "MATCH PARTIAL not yet implemented",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) ON UPDATE SET NULL (a))",
            sqlstate: "0A000",
            message: "a column list with SET NULL is only supported for ON DELETE actions",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) ON UPDATE SET DEFAULT (a))",
            sqlstate: "0A000",
            message: "a column list with SET DEFAULT is only supported for ON DELETE actions",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ON UPDATE SET NULL (a))",
            sqlstate: "0A000",
            message: "a column list with SET NULL is only supported for ON DELETE actions",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) NOT DEFERRABLE INITIALLY DEFERRED)",
            sqlstate: "42601",
            message: "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) INITIALLY DEFERRED NOT DEFERRABLE)",
            sqlstate: "42601",
            message: "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) DEFERRABLE NOT DEFERRABLE)",
            sqlstate: "42601",
            message: "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed",
        },
        RefusalCase {
            sql: "CREATE TABLE c (a int REFERENCES p (id) INITIALLY DEFERRED INITIALLY IMMEDIATE)",
            sqlstate: "42601",
            message: "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed",
        },
    ];

    for case in cases {
        let error = err(case.sql);
        assert!(error.sqlstate() == case.sqlstate, "{}", case.sql);
        assert!(error.message == case.message, "{}", case.sql);
    }
}

// A referential action for a side already given is a syntax error, as it is in
// PostgreSQL's grammar — which admits at most one clause per side.
#[test]
fn a_repeated_referential_action_clause_is_a_syntax_error() {
    for sql in [
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE CASCADE ON DELETE RESTRICT)",
        "CREATE TABLE c (a int REFERENCES p (id) ON UPDATE CASCADE ON UPDATE RESTRICT)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE CASCADE ON UPDATE RESTRICT ON DELETE \
         CASCADE)",
        "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE ON DELETE \
         CASCADE)",
        "CREATE TABLE c (a int REFERENCES p (id) MATCH SIMPLE MATCH FULL)",
    ] {
        let error = err(sql);
        assert!(error.sqlstate() == "42601", "{sql}");
    }
}

// A malformed action or MATCH body is rejected rather than skipped.
#[test]
fn a_malformed_reference_tail_is_rejected() {
    for sql in [
        "CREATE TABLE c (a int REFERENCES p (id) MATCH)",
        "CREATE TABLE c (a int REFERENCES p (id) MATCH LOOSE)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE SET)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE SET SOMETHING)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE NO)",
        "CREATE TABLE c (a int REFERENCES p (id) ON DELETE ANNIHILATE)",
        "CREATE TABLE c (a int REFERENCES p (id) INITIALLY)",
        "CREATE TABLE c (a int REFERENCES p (id) INITIALLY SOMETIME)",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

struct AttributeCase {
    tail: &'static str,
    expected: ConstraintAttributes,
}

// Every deferrability spelling and combination. `INITIALLY DEFERRED` written
// alone implies `DEFERRABLE`, exactly as PostgreSQL stores it
// (`condeferrable` is true for `INITIALLY DEFERRED` with no `DEFERRABLE`).
#[test]
fn every_deferrability_spelling_reaches_the_constraint_attributes() {
    let cases = &[
        AttributeCase {
            tail: "",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: false,
                initially_deferred: false,
            },
        },
        AttributeCase {
            tail: "NOT DEFERRABLE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: false,
                initially_deferred: false,
            },
        },
        AttributeCase {
            tail: "DEFERRABLE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: true,
                initially_deferred: false,
            },
        },
        AttributeCase {
            tail: "DEFERRABLE INITIALLY IMMEDIATE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: true,
                initially_deferred: false,
            },
        },
        AttributeCase {
            tail: "DEFERRABLE INITIALLY DEFERRED",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: true,
                initially_deferred: true,
            },
        },
        AttributeCase {
            tail: "INITIALLY DEFERRED",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: true,
                initially_deferred: true,
            },
        },
        AttributeCase {
            tail: "INITIALLY DEFERRED DEFERRABLE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: true,
                initially_deferred: true,
            },
        },
        AttributeCase {
            tail: "INITIALLY IMMEDIATE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: false,
                initially_deferred: false,
            },
        },
        AttributeCase {
            tail: "NOT DEFERRABLE INITIALLY IMMEDIATE",
            expected: ConstraintAttributes {
                not_valid: false,
                deferrable: false,
                initially_deferred: false,
            },
        },
    ];

    for case in cases {
        let sql = format!(
            "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) {})",
            case.tail
        );
        assert!(table_constraint(&sql).attributes == case.expected, "{sql}");

        let sql = format!("CREATE TABLE c (a int REFERENCES p (id) {})", case.tail);
        assert!(column_constraint(&sql).attributes == case.expected, "{sql}");
    }
}

// `NOT VALID` belongs to the table-constraint grammar, and composes with the
// deferrability clauses in either order.
#[test]
fn not_valid_reaches_the_table_constraint_and_is_barred_from_a_column_one() {
    for tail in ["NOT VALID", "DEFERRABLE INITIALLY DEFERRED NOT VALID"] {
        let deferred = tail.contains("DEFERRED");
        let sql = format!("CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) {tail})");
        assert!(
            table_constraint(&sql).attributes
                == ConstraintAttributes {
                    not_valid: true,
                    deferrable: deferred,
                    initially_deferred: deferred,
                },
            "{sql}"
        );
    }
    assert!(parse("CREATE TABLE c (a int REFERENCES p (id) NOT VALID)").is_err());
}

// An explicit `CONSTRAINT <name>` label rides along with the whole clause.
#[test]
fn a_named_foreign_key_carries_its_name_and_its_whole_tail() {
    let constraint = table_constraint(
        "CREATE TABLE c (a int, b int, CONSTRAINT c_fk FOREIGN KEY (a, b) REFERENCES p (x, y) \
         MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL (b) DEFERRABLE INITIALLY DEFERRED NOT \
         VALID)",
    );
    assert!(
        constraint
            == TableConstraint {
                name: Some("c_fk".into()),
                kind: TableConstraintKind::ForeignKey {
                    columns: vec!["a".into(), "b".into()],
                    references: ForeignKeyRef {
                        table: "p".into(),
                        columns: vec!["x".into(), "y".into()],
                        match_type: MatchType::Full,
                        on_delete: ReferentialAction::SetNull,
                        on_update: ReferentialAction::Cascade,
                        set_columns: vec!["b".into()],
                    },
                },
                attributes: ConstraintAttributes {
                    not_valid: true,
                    deferrable: true,
                    initially_deferred: true,
                },
            }
    );
}

// `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` reaches the same AST as the
// `CREATE TABLE` spelling.
#[test]
fn alter_table_add_foreign_key_carries_the_whole_clause() {
    use crabka_pgparser::ast::AlterTableAction;

    let Statement::AlterTable { actions, .. } = one(
        "ALTER TABLE c ADD CONSTRAINT c_fk FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE \
         DEFERRABLE INITIALLY DEFERRED NOT VALID",
    ) else {
        panic!("expected ALTER TABLE");
    };
    assert!(
        actions
            == vec![AlterTableAction::AddConstraint(TableConstraint {
                name: Some("c_fk".into()),
                kind: TableConstraintKind::ForeignKey {
                    columns: vec!["a".into()],
                    references: ForeignKeyRef {
                        table: "p".into(),
                        columns: vec!["id".into()],
                        match_type: MatchType::Simple,
                        on_delete: ReferentialAction::Cascade,
                        on_update: ReferentialAction::NoAction,
                        set_columns: Vec::new(),
                    },
                },
                attributes: ConstraintAttributes {
                    not_valid: true,
                    deferrable: true,
                    initially_deferred: true,
                },
            })]
    );
}

// `SET CONSTRAINTS` keeps its target list and its mode.
#[test]
fn set_constraints_carries_its_names_and_mode() {
    struct Case {
        sql: &'static str,
        names: Option<&'static [&'static str]>,
        deferred: bool,
    }
    let cases = &[
        Case {
            sql: "SET CONSTRAINTS ALL DEFERRED",
            names: None,
            deferred: true,
        },
        Case {
            sql: "SET CONSTRAINTS ALL IMMEDIATE",
            names: None,
            deferred: false,
        },
        Case {
            sql: "SET CONSTRAINTS c_fk DEFERRED",
            names: Some(&["c_fk"]),
            deferred: true,
        },
        Case {
            sql: "set constraints c_fk, other_fk immediate",
            names: Some(&["c_fk", "other_fk"]),
            deferred: false,
        },
    ];
    for case in cases {
        assert!(
            one(case.sql)
                == Statement::Utility(UtilityStatement::SetConstraints {
                    names: case
                        .names
                        .map(|names| names.iter().map(|&n| n.into()).collect()),
                    deferred: case.deferred,
                }),
            "{}",
            case.sql
        );
    }
    for sql in ["SET CONSTRAINTS ALL", "SET CONSTRAINTS DEFERRED"] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
