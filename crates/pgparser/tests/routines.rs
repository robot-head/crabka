//! P2: the SQL-routine grammar for `CREATE`/`ALTER`/`DROP` of `FUNCTION`,
//! `PROCEDURE` and `ROUTINE`, plus `CALL` and `DO`.

use assert2::assert;
use crabka_pgparser::{
    ast::{
        AlterRoutineAction, CreateRoutineStmt, RoutineArg, RoutineArgMode, RoutineBody,
        RoutineObject, RoutineOption, RoutineParallel, RoutineReturn, RoutineSignature,
        RoutineTableColumn, RoutineType, RoutineVolatility, Statement,
    },
    command::CommandIdentity,
    parse, parse_with_command_identities,
};
use crabka_pgtypes::ColumnType;

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(statements.len() == 1);
    statements.remove(0)
}

fn create(sql: &str) -> CreateRoutineStmt {
    match one(sql) {
        Statement::CreateRoutine(stmt) => *stmt,
        other => panic!("{sql} parsed as {other:?}"),
    }
}

fn identity(sql: &str) -> CommandIdentity {
    let mut parsed =
        parse_with_command_identities(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(parsed.len() == 1);
    parsed.remove(0).1
}

fn int_arg(name: &str) -> RoutineArg {
    RoutineArg {
        name: Some(name.into()),
        mode: RoutineArgMode::In,
        ty: RoutineType::builtin(ColumnType::Int4, "integer".into()),
        default: None,
    }
}

#[test]
fn parses_a_minimal_sql_function() {
    let stmt =
        create("CREATE FUNCTION add2(a int, b int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql");
    assert!(
        stmt == CreateRoutineStmt {
            name: "add2".into(),
            object: RoutineObject::Function,
            or_replace: false,
            args: vec![int_arg("a"), int_arg("b")],
            returns: RoutineReturn::Type {
                ty: RoutineType::builtin(ColumnType::Int4, "integer".into()),
                setof: false,
            },
            options: vec![
                RoutineOption::Body(RoutineBody::Source("SELECT $1 + $2".into())),
                RoutineOption::Language("sql".into()),
            ],
        }
    );
}

#[test]
fn distinguishes_a_bare_type_from_a_named_parameter() {
    let stmt =
        create("CREATE FUNCTION f(int, b double precision, text[]) RETURNS int AS '' LANGUAGE sql");
    let modes: Vec<Option<String>> = stmt.args.iter().map(|a| a.name.clone()).collect();
    assert!(modes == vec![None, Some("b".to_string()), None]);
    let types: Vec<String> = stmt.args.iter().map(|a| a.ty.name.clone()).collect();
    assert!(types == vec!["integer", "double precision", "text[]"]);
}

#[test]
fn parses_every_parameter_mode_and_a_default() {
    let stmt = create(
        "CREATE FUNCTION f(IN a int, OUT b int, INOUT c int, VARIADIC d int[], e int DEFAULT 3) \
         RETURNS int AS '' LANGUAGE sql",
    );
    let modes: Vec<RoutineArgMode> = stmt.args.iter().map(|a| a.mode).collect();
    assert!(
        modes
            == vec![
                RoutineArgMode::In,
                RoutineArgMode::Out,
                RoutineArgMode::InOut,
                RoutineArgMode::Variadic,
                RoutineArgMode::In,
            ]
    );
    assert!(stmt.args[4].default.is_some());
    assert!(stmt.args.iter().take(4).all(|a| a.default.is_none()));
}

#[test]
fn parses_the_equals_spelling_of_a_parameter_default() {
    let stmt = create("CREATE FUNCTION f(a int = 7) RETURNS int AS '' LANGUAGE sql");
    assert!(stmt.args[0].default.is_some());
}

#[test]
fn parses_every_return_shape() {
    let cases: Vec<(&str, RoutineReturn)> = vec![
        (
            "RETURNS int",
            RoutineReturn::Type {
                ty: RoutineType::builtin(ColumnType::Int4, "integer".into()),
                setof: false,
            },
        ),
        (
            "RETURNS SETOF text",
            RoutineReturn::Type {
                ty: RoutineType::builtin(ColumnType::Text, "text".into()),
                setof: true,
            },
        ),
        (
            "RETURNS void",
            RoutineReturn::Type {
                ty: RoutineType::named("void".into()),
                setof: false,
            },
        ),
        (
            "RETURNS TABLE(a int, b text)",
            RoutineReturn::Table(vec![
                RoutineTableColumn {
                    name: "a".into(),
                    ty: RoutineType::builtin(ColumnType::Int4, "integer".into()),
                },
                RoutineTableColumn {
                    name: "b".into(),
                    ty: RoutineType::builtin(ColumnType::Text, "text".into()),
                },
            ]),
        ),
    ];
    for (clause, want) in cases {
        let sql = format!("CREATE FUNCTION f() {clause} AS '' LANGUAGE sql");
        assert!(create(&sql).returns == want, "{clause}");
    }
}

#[test]
fn a_procedure_has_no_returns_clause() {
    let stmt = create("CREATE PROCEDURE p(x int) LANGUAGE sql AS $$ SELECT x $$");
    assert!(stmt.object == RoutineObject::Procedure);
    assert!(stmt.returns == RoutineReturn::Unspecified);
}

#[test]
fn parses_or_replace() {
    let stmt = create("CREATE OR REPLACE FUNCTION f() RETURNS int AS '' LANGUAGE sql");
    assert!(stmt.or_replace);
    assert!(!create("CREATE FUNCTION f() RETURNS int AS '' LANGUAGE sql").or_replace);
}

#[test]
fn parses_every_definition_qualifier() {
    let stmt = create(
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE \
         SECURITY DEFINER LEAKPROOF COST 5 ROWS 10 SET work_mem = '64MB' AS 'SELECT 1'",
    );
    assert!(
        stmt.options
            == vec![
                RoutineOption::Language("sql".into()),
                RoutineOption::Volatility(RoutineVolatility::Immutable),
                RoutineOption::Strict(true),
                RoutineOption::Parallel(RoutineParallel::Safe),
                RoutineOption::SecurityDefiner(true),
                RoutineOption::Leakproof(true),
                RoutineOption::Cost(5.0),
                RoutineOption::Rows(10.0),
                RoutineOption::Set {
                    name: "work_mem".into(),
                    value: Some("64MB".into()),
                    source: "work_mem = '64MB'".into(),
                },
                RoutineOption::Body(RoutineBody::Source("SELECT 1".into())),
            ]
    );
}

#[test]
fn parses_the_long_strictness_spellings() {
    let strict = create(
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql RETURNS NULL ON NULL INPUT AS 'SELECT 1'",
    );
    assert!(strict.options.contains(&RoutineOption::Strict(true)));
    let lax =
        create("CREATE FUNCTION f() RETURNS int LANGUAGE sql CALLED ON NULL INPUT AS 'SELECT 1'");
    assert!(lax.options.contains(&RoutineOption::Strict(false)));
    let not_leakproof =
        create("CREATE FUNCTION f() RETURNS int LANGUAGE sql NOT LEAKPROOF AS 'SELECT 1'");
    assert!(
        not_leakproof
            .options
            .contains(&RoutineOption::Leakproof(false))
    );
    let invoker = create(
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql EXTERNAL SECURITY INVOKER AS 'SELECT 1'",
    );
    assert!(
        invoker
            .options
            .contains(&RoutineOption::SecurityDefiner(false))
    );
}

#[test]
fn parses_the_begin_atomic_sql_body() {
    let stmt =
        create("CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT a + 1; END");
    let RoutineOption::Body(RoutineBody::Atomic { statements, text }) = &stmt.options[1] else {
        panic!("expected an atomic body, got {:?}", stmt.options);
    };
    assert!(statements.len() == 1);
    assert!(text == "SELECT a + 1");
}

#[test]
fn parses_a_multi_statement_begin_atomic_body() {
    let stmt =
        create("CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; SELECT 2; END");
    let RoutineOption::Body(RoutineBody::Atomic { statements, .. }) = &stmt.options[1] else {
        panic!("expected an atomic body");
    };
    assert!(statements.len() == 2);
}

#[test]
fn parses_redundant_semicolons_in_a_begin_atomic_body() {
    let stmt = create(
        "CREATE FUNCTION f() RETURNS boolean LANGUAGE sql BEGIN ATOMIC ;;RETURN false;; END",
    );
    let RoutineOption::Body(RoutineBody::Atomic { statements, text }) = &stmt.options[1] else {
        panic!("expected an atomic body");
    };
    assert!(statements.len() == 1);
    assert!(text == ";;RETURN false");
}

#[test]
fn parses_the_return_sql_body() {
    let stmt = create("CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql RETURN a + 1");
    let RoutineOption::Body(RoutineBody::Return { text, .. }) = &stmt.options[1] else {
        panic!("expected a RETURN body, got {:?}", stmt.options);
    };
    assert!(text == "a + 1");
}

#[test]
fn parses_drop_in_every_spelling() {
    let cases: Vec<(&str, RoutineObject, bool, bool, usize)> = vec![
        ("DROP FUNCTION f", RoutineObject::Function, false, false, 1),
        (
            "DROP PROCEDURE IF EXISTS p(int)",
            RoutineObject::Procedure,
            true,
            false,
            1,
        ),
        (
            "DROP ROUTINE f(int), g(text) CASCADE",
            RoutineObject::Routine,
            false,
            true,
            2,
        ),
        (
            "DROP FUNCTION f(int) RESTRICT",
            RoutineObject::Function,
            false,
            false,
            1,
        ),
    ];
    for (sql, want_object, want_if_exists, want_cascade, want_len) in cases {
        let Statement::DropRoutine {
            object,
            if_exists,
            routines,
            cascade,
        } = one(sql)
        else {
            panic!("{sql} is not a DROP ROUTINE");
        };
        assert!(object == want_object, "{sql}");
        assert!(if_exists == want_if_exists, "{sql}");
        assert!(cascade == want_cascade, "{sql}");
        assert!(routines.len() == want_len, "{sql}");
    }
}

#[test]
fn a_drop_without_parentheses_carries_no_argument_list() {
    let Statement::DropRoutine { routines, .. } = one("DROP FUNCTION f") else {
        panic!("not a DROP ROUTINE");
    };
    assert!(
        routines
            == vec![RoutineSignature {
                name: "f".into(),
                args: None
            }]
    );
    let Statement::DropRoutine { routines, .. } = one("DROP FUNCTION f()") else {
        panic!("not a DROP ROUTINE");
    };
    assert!(
        routines
            == vec![RoutineSignature {
                name: "f".into(),
                args: Some(Vec::new()),
            }]
    );
}

#[test]
fn parses_every_alter_action() {
    let cases: Vec<(&str, AlterRoutineAction)> = vec![
        (
            "ALTER FUNCTION f(int) RENAME TO g",
            AlterRoutineAction::RenameTo("g".into()),
        ),
        (
            "ALTER FUNCTION f(int) OWNER TO bob",
            AlterRoutineAction::OwnerTo("bob".into()),
        ),
        (
            "ALTER ROUTINE f(int) SET SCHEMA public",
            AlterRoutineAction::SetSchema("public".into()),
        ),
        (
            "ALTER PROCEDURE p(int) DEPENDS ON EXTENSION ext",
            AlterRoutineAction::DependsOnExtension {
                name: "ext".into(),
                no: false,
            },
        ),
        (
            "ALTER FUNCTION f(int) NO DEPENDS ON EXTENSION ext",
            AlterRoutineAction::DependsOnExtension {
                name: "ext".into(),
                no: true,
            },
        ),
        (
            "ALTER FUNCTION f(int) IMMUTABLE STRICT",
            AlterRoutineAction::Options(vec![
                RoutineOption::Volatility(RoutineVolatility::Immutable),
                RoutineOption::Strict(true),
            ]),
        ),
        (
            "ALTER FUNCTION f(int) SET search_path TO public",
            AlterRoutineAction::Options(vec![RoutineOption::Set {
                name: "search_path".into(),
                value: Some("public".into()),
                source: "search_path TO public".into(),
            }]),
        ),
        (
            "ALTER FUNCTION f(int) RESET ALL",
            AlterRoutineAction::Options(vec![RoutineOption::Set {
                name: "all".into(),
                value: None,
                source: "ALL".into(),
            }]),
        ),
    ];
    for (sql, want) in cases {
        let Statement::AlterRoutine { action, .. } = one(sql) else {
            panic!("{sql} is not an ALTER ROUTINE");
        };
        assert!(action == want, "{sql}");
    }
}

#[test]
fn parses_call_and_do() {
    assert!(
        one("CALL p(1, 'x')")
            == Statement::Call {
                name: "p".into(),
                args: vec![
                    crabka_pgparser::ast::Expr::IntLiteral("1".into()),
                    crabka_pgparser::ast::Expr::StringLiteral("x".into()),
                ],
                named_args: Vec::new(),
                variadic: None,
            }
    );
    assert!(
        one("CALL p()")
            == Statement::Call {
                name: "p".into(),
                args: Vec::new(),
                named_args: Vec::new(),
                variadic: None,
            }
    );
    assert!(matches!(
        one("CALL p(b => 2, a => 1)"),
        Statement::Call { args, named_args, variadic, .. }
            if args.is_empty()
                && matches!(named_args.as_slice(), [(b, _), (a, _)] if b == "b" && a == "a")
                && variadic.is_none()
    ));
    assert!(matches!(
        one("CALL p(1, VARIADIC ARRAY[2, 3])"),
        Statement::Call { args, named_args, variadic, .. }
            if args.len() == 1 && named_args.is_empty() && variadic.is_some()
    ));
    assert!(
        one("DO $$ SELECT 1 $$")
            == Statement::DoBlock {
                language: "plpgsql".into(),
                body: " SELECT 1 ".into(),
            }
    );
    assert!(
        one("DO LANGUAGE sql $$ SELECT 1 $$")
            == Statement::DoBlock {
                language: "sql".into(),
                body: " SELECT 1 ".into(),
            }
    );
    assert!(
        one("DO $$ SELECT 1 $$ LANGUAGE sql")
            == Statement::DoBlock {
                language: "sql".into(),
                body: " SELECT 1 ".into(),
            }
    );
}

#[test]
fn reports_the_pg18_command_identity() {
    let cases = [
        (
            "CREATE FUNCTION f() RETURNS int AS '' LANGUAGE sql",
            CommandIdentity::CreateFunction,
        ),
        (
            "CREATE OR REPLACE FUNCTION f() RETURNS int AS '' LANGUAGE sql",
            CommandIdentity::CreateFunction,
        ),
        (
            "CREATE PROCEDURE p() LANGUAGE sql AS ''",
            CommandIdentity::CreateProcedure,
        ),
        (
            "ALTER FUNCTION f(int) RENAME TO g",
            CommandIdentity::AlterFunction,
        ),
        (
            "ALTER PROCEDURE p(int) RENAME TO g",
            CommandIdentity::AlterProcedure,
        ),
        (
            "ALTER ROUTINE f(int) RENAME TO g",
            CommandIdentity::AlterRoutine,
        ),
        ("DROP FUNCTION f(int)", CommandIdentity::DropFunction),
        ("DROP PROCEDURE p(int)", CommandIdentity::DropProcedure),
        ("DROP ROUTINE f(int)", CommandIdentity::DropRoutine),
        ("CALL p()", CommandIdentity::Call),
        ("DO $$ SELECT 1 $$", CommandIdentity::Do),
    ];
    for (sql, want) in cases {
        assert!(identity(sql) == want, "{sql}");
    }
}

#[test]
fn a_schema_other_than_public_does_not_exist() {
    let error = parse("CREATE FUNCTION other.f() RETURNS int AS '' LANGUAGE sql")
        .expect_err("a missing schema is 3F000");
    assert!(error.sqlstate() == "3F000");
    assert!(error.message == "schema \"other\" does not exist");
    let qualified = create("CREATE FUNCTION public.f() RETURNS int AS '' LANGUAGE sql");
    assert!(qualified.name == "f");
}

#[test]
fn an_unknown_parameter_type_is_carried_through_by_name() {
    let stmt = create("CREATE FUNCTION f(x mytable) RETURNS SETOF mytable AS '' LANGUAGE sql");
    assert!(stmt.args[0].ty == RoutineType::named("mytable".into()));
    assert!(
        stmt.returns
            == RoutineReturn::Type {
                ty: RoutineType::named("mytable".into()),
                setof: true,
            }
    );
}

#[test]
fn function_and_procedure_remain_usable_as_identifiers() {
    assert!(parse("SELECT function FROM procedure").is_ok());
    assert!(parse("SELECT call, do FROM routine").is_ok());
}
