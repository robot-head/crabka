//! `CREATE`/`ALTER`/`DROP POLICY` and the four `ALTER TABLE` row-security
//! subcommands.
//!
//! A policy's quals are captured twice — parsed and as written — because the
//! catalog stores the text (`pg_policy.polqual` hands it straight back) and the
//! executor evaluates the expression. The tests below pin both, since a
//! production that kept only one of them would look right until somebody read
//! `pg_policies`.

use assert2::assert;
use crabka_pgparser::{
    ast::{
        AlterPolicyAction, AlterTableAction, BinaryOp, CreatePolicy, Expr, PolicyCommand,
        PolicyQual, RelationRef, Statement,
    },
    parse,
};

fn statement(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|error| panic!("parse `{sql}`: {error}"));
    assert!(statements.len() == 1, "`{sql}` should be one statement");
    statements.pop().expect("one statement")
}

fn document() -> RelationRef {
    RelationRef::bare("document")
}

fn qual(source: &str, expr: Expr) -> PolicyQual {
    PolicyQual {
        expr,
        source: source.into(),
    }
}

fn column_eq_literal(column: &str, literal: &str) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column {
            table: None,
            name: column.into(),
        }),
        right: Box::new(Expr::StringLiteral(literal.into())),
    }
}

/// Every clause `CREATE POLICY` accepts, and its default when omitted.
#[test]
fn create_policy_carries_every_clause() {
    struct Case {
        sql: &'static str,
        expected: CreatePolicy,
    }
    let bare = CreatePolicy {
        name: "p".into(),
        table: document(),
        permissive: true,
        command: PolicyCommand::All,
        roles: Vec::new(),
        using: None,
        with_check: None,
    };
    let cases = [
        Case {
            sql: "CREATE POLICY p ON document",
            expected: bare.clone(),
        },
        Case {
            sql: "CREATE POLICY p ON document AS RESTRICTIVE",
            expected: CreatePolicy {
                permissive: false,
                ..bare.clone()
            },
        },
        Case {
            sql: "CREATE POLICY p ON document AS PERMISSIVE FOR SELECT",
            expected: CreatePolicy {
                command: PolicyCommand::Select,
                ..bare.clone()
            },
        },
        Case {
            sql: "CREATE POLICY p ON document FOR UPDATE TO alice, bob",
            expected: CreatePolicy {
                command: PolicyCommand::Update,
                roles: vec!["alice".into(), "bob".into()],
                ..bare.clone()
            },
        },
        Case {
            // `TO PUBLIC` is the empty list, which is how the catalog and
            // PostgreSQL both spell "every role".
            sql: "CREATE POLICY p ON document TO PUBLIC",
            expected: bare.clone(),
        },
        Case {
            sql: "CREATE POLICY p ON document FOR INSERT WITH CHECK (holder = 'alice')",
            expected: CreatePolicy {
                command: PolicyCommand::Insert,
                with_check: Some(qual(
                    "holder = 'alice'",
                    column_eq_literal("holder", "alice"),
                )),
                ..bare.clone()
            },
        },
        Case {
            sql: "CREATE POLICY p ON document FOR ALL USING (holder = 'alice') \
                  WITH CHECK (holder = 'bob')",
            expected: CreatePolicy {
                using: Some(qual(
                    "holder = 'alice'",
                    column_eq_literal("holder", "alice"),
                )),
                with_check: Some(qual("holder = 'bob'", column_eq_literal("holder", "bob"))),
                ..bare.clone()
            },
        },
        Case {
            sql: "CREATE POLICY p ON document FOR DELETE",
            expected: CreatePolicy {
                command: PolicyCommand::Delete,
                ..bare
            },
        },
    ];
    for case in cases {
        assert!(
            statement(case.sql) == Statement::CreatePolicy(case.expected),
            "{}",
            case.sql
        );
    }
}

/// The qual's stored source is the text between the parentheses, verbatim —
/// including whatever spacing and casing the author used, because `pg_get_expr`
/// hands it back.
#[test]
fn a_qual_keeps_the_source_text_it_was_written_with() {
    let Statement::CreatePolicy(policy) =
        statement("CREATE POLICY p ON document USING (  holder  =  'alice'  )")
    else {
        panic!("expected CREATE POLICY");
    };
    assert!(policy.using.expect("a USING qual").source == "holder  =  'alice'");
}

#[test]
fn alter_policy_covers_the_rename_and_the_change_forms() {
    assert!(
        statement("ALTER POLICY p ON document RENAME TO q")
            == Statement::AlterPolicy {
                name: "p".into(),
                table: document(),
                action: AlterPolicyAction::RenameTo("q".into()),
            }
    );

    let Statement::AlterPolicy { action, .. } =
        statement("ALTER POLICY p ON document TO alice USING (holder = 'alice')")
    else {
        panic!("expected ALTER POLICY");
    };
    let AlterPolicyAction::Change(change) = action else {
        panic!("expected the change form");
    };
    assert!(change.roles == Some(vec!["alice".into()]));
    assert!(
        change.using
            == Some(qual(
                "holder = 'alice'",
                column_eq_literal("holder", "alice")
            ))
    );
    assert!(change.with_check.is_none());

    // An omitted clause is `None`, which leaves the stored value alone — an
    // `ALTER POLICY` that wrote `None` as "remove the qual" would quietly widen
    // the policy to everything.
    let Statement::AlterPolicy { action, .. } = statement("ALTER POLICY p ON document TO PUBLIC")
    else {
        panic!("expected ALTER POLICY");
    };
    let AlterPolicyAction::Change(change) = action else {
        panic!("expected the change form");
    };
    assert!(change.roles == Some(Vec::new()));
    assert!(change.using.is_none() && change.with_check.is_none());
}

#[test]
fn drop_policy_carries_if_exists_and_the_drop_behavior() {
    for (sql, if_exists, cascade) in [
        ("DROP POLICY p ON document", false, false),
        ("DROP POLICY IF EXISTS p ON document", true, false),
        ("DROP POLICY p ON document CASCADE", false, true),
        ("DROP POLICY IF EXISTS p ON document RESTRICT", true, false),
    ] {
        assert!(
            statement(sql)
                == Statement::DropPolicy {
                    name: "p".into(),
                    table: document(),
                    if_exists,
                    cascade,
                },
            "{sql}"
        );
    }
}

/// The four row-security subcommands, and — the reason they are placed where
/// they are — the `ENABLE …` subcommands that must keep parsing as they did.
#[test]
fn the_row_security_subcommands_parse_ahead_of_the_catch_all() {
    for (sql, expected) in [
        (
            "ALTER TABLE document ENABLE ROW LEVEL SECURITY",
            AlterTableAction::EnableRowSecurity,
        ),
        (
            "ALTER TABLE document DISABLE ROW LEVEL SECURITY",
            AlterTableAction::DisableRowSecurity,
        ),
        (
            "ALTER TABLE document FORCE ROW LEVEL SECURITY",
            AlterTableAction::ForceRowSecurity,
        ),
        (
            "ALTER TABLE document NO FORCE ROW LEVEL SECURITY",
            AlterTableAction::NoForceRowSecurity,
        ),
    ] {
        let Statement::AlterTable { actions, .. } = statement(sql) else {
            panic!("expected ALTER TABLE from `{sql}`");
        };
        assert!(actions == vec![expected], "{sql}");
    }

    // A subcommand that merely starts with the same word is untouched.
    let Statement::AlterTable { actions, .. } =
        statement("ALTER TABLE document ENABLE TRIGGER ALL")
    else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(
        actions.as_slice(),
        [AlterTableAction::SetTriggerMode { .. }]
    ));
}
