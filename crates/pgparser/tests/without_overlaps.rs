//! The `WITHOUT OVERLAPS` and `PERIOD` grammar of `PostgreSQL` 18's temporal
//! keys: both markers ride on the last element of a key list, and both spell
//! themselves with words that remain ordinary identifiers everywhere else.

use assert2::assert;
use crabka_pgparser::{
    ast::{AlterTableAction, Statement, TableConstraintKind},
    parse,
};

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(statements.len() == 1);
    statements.remove(0)
}

/// The single table constraint of a one-constraint `CREATE TABLE`.
fn table_constraint(sql: &str) -> TableConstraintKind {
    match one(sql) {
        Statement::CreateTable { constraints, .. } => {
            assert!(constraints.len() == 1, "{sql}");
            constraints.into_iter().next().expect("one constraint").kind
        }
        other => panic!("{sql} parsed as {other:?}"),
    }
}

/// The single constraint of a one-action `ALTER TABLE … ADD CONSTRAINT`.
fn added_constraint(sql: &str) -> TableConstraintKind {
    match one(sql) {
        Statement::AlterTable { mut actions, .. } => {
            assert!(actions.len() == 1, "{sql}");
            match actions.remove(0) {
                AlterTableAction::AddConstraint(constraint) => constraint.kind,
                other => panic!("{sql} parsed as {other:?}"),
            }
        }
        other => panic!("{sql} parsed as {other:?}"),
    }
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

// `WITHOUT OVERLAPS` sets a flag on the key rather than becoming a column of
// its own, and it reaches the same AST from `CREATE TABLE` and `ALTER TABLE`.
#[test]
fn without_overlaps_marks_the_key_and_not_a_column() {
    assert!(
        table_constraint(
            "CREATE TABLE t (a int4range, b daterange, PRIMARY KEY (a, b WITHOUT OVERLAPS))"
        ) == TableConstraintKind::PrimaryKey {
            columns: columns(&["a", "b"]),
            without_overlaps: true,
        }
    );
    assert!(
        added_constraint("ALTER TABLE t ADD PRIMARY KEY (a, b WITHOUT OVERLAPS)")
            == TableConstraintKind::PrimaryKey {
                columns: columns(&["a", "b"]),
                without_overlaps: true,
            }
    );
    assert!(
        table_constraint(
            "CREATE TABLE t (a int4range, b daterange, UNIQUE (a, b WITHOUT OVERLAPS))"
        ) == TableConstraintKind::Unique {
            columns: columns(&["a", "b"]),
            nulls_not_distinct: false,
            without_overlaps: true,
        }
    );
    // Absent, the flag is false and the key list is unchanged.
    assert!(
        table_constraint("CREATE TABLE t (a int4range, b daterange, PRIMARY KEY (a, b))")
            == TableConstraintKind::PrimaryKey {
                columns: columns(&["a", "b"]),
                without_overlaps: false,
            }
    );
}

// The clause closes the key list, so a column after it — or a second one — is a
// syntax error, exactly as upstream's grammar reports.
#[test]
fn without_overlaps_must_be_last() {
    for sql in [
        "CREATE TABLE t (a int4range, b daterange, PRIMARY KEY (b WITHOUT OVERLAPS, a))",
        "CREATE TABLE t (a int4range, b daterange, UNIQUE (b WITHOUT OVERLAPS, a))",
        "CREATE TABLE t (a int4range, b daterange, PRIMARY KEY (a WITHOUT OVERLAPS, b WITHOUT OVERLAPS))",
        "ALTER TABLE t ADD PRIMARY KEY (b WITHOUT OVERLAPS, a)",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

// `PERIOD` marks the last column on either side of a foreign key, and the two
// sides record it independently — mismatches are a semantic refusal, not a
// parse failure.
#[test]
fn period_marks_the_last_foreign_key_column_on_each_side() {
    struct Case {
        sql: &'static str,
        referencing: Vec<String>,
        period: bool,
        referenced: Vec<String>,
        referenced_period: bool,
    }
    let cases = [
        Case {
            sql: "CREATE TABLE c (a int4range, b daterange, \
                  FOREIGN KEY (a, PERIOD b) REFERENCES p (x, PERIOD y))",
            referencing: columns(&["a", "b"]),
            period: true,
            referenced: columns(&["x", "y"]),
            referenced_period: true,
        },
        Case {
            sql: "CREATE TABLE c (a int4range, b daterange, \
                  FOREIGN KEY (a, PERIOD b) REFERENCES p (x, y))",
            referencing: columns(&["a", "b"]),
            period: true,
            referenced: columns(&["x", "y"]),
            referenced_period: false,
        },
        Case {
            sql: "CREATE TABLE c (a int4range, b daterange, \
                  FOREIGN KEY (a, b) REFERENCES p (x, PERIOD y))",
            referencing: columns(&["a", "b"]),
            period: false,
            referenced: columns(&["x", "y"]),
            referenced_period: true,
        },
        Case {
            sql: "CREATE TABLE c (a int4range, b daterange, \
                  FOREIGN KEY (a, b) REFERENCES p (x, y))",
            referencing: columns(&["a", "b"]),
            period: false,
            referenced: columns(&["x", "y"]),
            referenced_period: false,
        },
    ];
    for case in cases {
        let TableConstraintKind::ForeignKey {
            columns: referencing,
            period,
            references,
        } = table_constraint(case.sql)
        else {
            panic!("{} is not a foreign key", case.sql);
        };
        assert!(referencing == case.referencing, "{}", case.sql);
        assert!(period == case.period, "{}", case.sql);
        assert!(references.columns == case.referenced, "{}", case.sql);
        assert!(references.period == case.referenced_period, "{}", case.sql);
    }
}

// Neither marker is a reserved word: a column may still be called `period`,
// `without` or `overlaps`, and a lone `PERIOD` in a key list is a column name.
#[test]
fn the_marker_words_stay_ordinary_identifiers() {
    assert!(
        table_constraint("CREATE TABLE t (period int4range, PRIMARY KEY (period))")
            == TableConstraintKind::PrimaryKey {
                columns: columns(&["period"]),
                without_overlaps: false,
            }
    );
    let TableConstraintKind::ForeignKey {
        columns: referencing,
        period,
        references,
    } = table_constraint(
        "CREATE TABLE c (period int4range, FOREIGN KEY (period) REFERENCES p (period))",
    )
    else {
        panic!("not a foreign key");
    };
    assert!(referencing == columns(&["period"]));
    assert!(!period);
    assert!(references.columns == columns(&["period"]));
    assert!(!references.period);

    for sql in [
        "CREATE TABLE t (without int4range, overlaps daterange, PRIMARY KEY (without, overlaps))",
        "CREATE TABLE t (period text, without text)",
    ] {
        assert!(parse(sql).is_ok(), "{sql}");
    }
}
