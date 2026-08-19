//! The aggregate grammar — `CREATE`/`ALTER`/`DROP AGGREGATE`.
//!
//! Every SQL string here is drawn from `PostgreSQL` 18.4's own regression
//! corpus (`create_aggregate.sql`, `polymorphism.sql`, `alter_generic.sql`,
//! `aggregates.sql`, `window.sql`, `drop_if_exists.sql`), so what the tests pin
//! is what the corpus actually asks the parser to accept.

use assert2::assert;
use crabka_pgparser::{
    ParseError,
    ast::{
        AggregateArgs, AggregateOption, AggregateSignature, AlterRoutineAction,
        CreateAggregateStmt, Expr, RoutineArg, RoutineArgMode, RoutineType, Statement,
    },
    command::CommandIdentity,
    parse, parse_with_command_identities,
};
use crabka_pgtypes::{ColumnType, ElemType};

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(statements.len() == 1);
    statements.remove(0)
}

fn create(sql: &str) -> CreateAggregateStmt {
    match one(sql) {
        Statement::CreateAggregate(stmt) => *stmt,
        other => panic!("{sql} parsed as {other:?}"),
    }
}

fn options(sql: &str) -> Vec<AggregateOption> {
    create(sql).options
}

fn identity(sql: &str) -> CommandIdentity {
    let mut parsed =
        parse_with_command_identities(sql).unwrap_or_else(|e| panic!("{sql}: {}", e.message));
    assert!(parsed.len() == 1);
    parsed.remove(0).1
}

fn error(sql: &str) -> ParseError {
    match parse(sql) {
        Ok(statements) => panic!("{sql} parsed as {statements:?}"),
        Err(e) => e,
    }
}

/// A built-in type, spelled the way `PostgreSQL` spells it back.
fn builtin(ty: ColumnType) -> RoutineType {
    RoutineType::builtin(ty, ty.name().to_string())
}

/// A type name the parser cannot resolve, carried through for the executor.
fn named(name: &str) -> RoutineType {
    RoutineType::named(name.to_string())
}

/// A positional argument of `ty`, which is every argument an aggregate has.
fn arg(ty: RoutineType) -> RoutineArg {
    RoutineArg {
        name: None,
        mode: RoutineArgMode::In,
        ty,
        default: None,
    }
}

fn unimplemented(name: &str, value: &str) -> AggregateOption {
    AggregateOption::Unimplemented {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn signature(name: &str, args: AggregateArgs) -> AggregateSignature {
    AggregateSignature {
        name: name.to_string(),
        args,
    }
}

// ---------------------------------------------------------------------------
// CREATE AGGREGATE — the three argument spellings
// ---------------------------------------------------------------------------

#[test]
fn parses_the_new_style_definition_list() {
    let stmt = create(
        "CREATE AGGREGATE newavg (int4) (
            sfunc = int4_avg_accum, stype = _int8,
            finalfunc = int8_avg,
            initcond = '{0,0}'
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "newavg".into(),
            or_replace: false,
            args: Some(AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))])),
            options: vec![
                AggregateOption::SFunc("int4_avg_accum".into()),
                AggregateOption::SType(builtin(ColumnType::Array(ElemType::Int8))),
                AggregateOption::FinalFunc("int8_avg".into()),
                AggregateOption::InitCond(Some("{0,0}".into())),
            ],
        }
    );
}

#[test]
fn parses_the_zero_argument_star_form() {
    let stmt = create(
        "CREATE AGGREGATE newcnt (*) (
            sfunc = int8inc, stype = int8,
            initcond = '0', parallel = safe
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "newcnt".into(),
            or_replace: false,
            args: Some(AggregateArgs::Star),
            options: vec![
                AggregateOption::SFunc("int8inc".into()),
                AggregateOption::SType(builtin(ColumnType::Int8)),
                AggregateOption::InitCond(Some("0".into())),
                unimplemented("parallel", "safe"),
            ],
        }
    );
}

#[test]
fn parses_the_quoted_any_argument_form() {
    let stmt = create(
        "CREATE AGGREGATE newcnt (\"any\") (
            sfunc = int8inc_any, stype = int8,
            initcond = '0'
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "newcnt".into(),
            or_replace: false,
            args: Some(AggregateArgs::Args(vec![arg(named("any"))])),
            options: vec![
                AggregateOption::SFunc("int8inc_any".into()),
                AggregateOption::SType(builtin(ColumnType::Int8)),
                AggregateOption::InitCond(Some("0".into())),
            ],
        }
    );
}

#[test]
fn parses_the_old_style_basetype_form() {
    let stmt = create(
        "CREATE AGGREGATE newsum (
            sfunc1 = int4pl, basetype = int4, stype1 = int4,
            initcond1 = '0'
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "newsum".into(),
            or_replace: false,
            args: None,
            options: vec![
                AggregateOption::SFunc("int4pl".into()),
                AggregateOption::BaseType(Some(builtin(ColumnType::Int4))),
                AggregateOption::SType(builtin(ColumnType::Int4)),
                AggregateOption::InitCond(Some("0".into())),
            ],
        }
    );
}

#[test]
fn reads_every_spelling_of_an_absent_basetype_as_none() {
    // `'ANY'` is the corpus spelling (`oldcnt`); the other two are the same
    // value written without the quotes PostgreSQL also accepts.
    for basetype in ["'ANY'", "\"any\"", "ANY"] {
        let sql = format!(
            "CREATE AGGREGATE oldcnt (sfunc = int8inc, basetype = {basetype}, stype = int8)"
        );
        assert!(
            options(&sql)
                == vec![
                    AggregateOption::SFunc("int8inc".into()),
                    AggregateOption::BaseType(None),
                    AggregateOption::SType(builtin(ColumnType::Int8)),
                ]
        );
    }
}

#[test]
fn a_single_bare_type_is_the_new_form_not_the_old_one() {
    // `a (int4)` has no `option =` pair, so it is an argument list; only an
    // `option =` pair after the `(` makes the list the old-style definition.
    assert!(
        create("create aggregate least_agg(int8) (stype = int8, sfunc = least_accum)").args
            == Some(AggregateArgs::Args(vec![arg(builtin(ColumnType::Int8))]))
    );
    assert!(
        create("CREATE AGGREGATE newcnt1 (sfunc = int4inc, stype = int4, initcond = '0')").args
            == None
    );
}

#[test]
fn parses_a_multi_argument_aggregate() {
    let stmt = create(
        "create aggregate sum2(int8,int8) (
            sfunc = sum3, stype = int8,
            initcond = '0'
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "sum2".into(),
            or_replace: false,
            args: Some(AggregateArgs::Args(vec![
                arg(builtin(ColumnType::Int8)),
                arg(builtin(ColumnType::Int8)),
            ])),
            options: vec![
                AggregateOption::SFunc("sum3".into()),
                AggregateOption::SType(builtin(ColumnType::Int8)),
                AggregateOption::InitCond(Some("0".into())),
            ],
        }
    );
}

#[test]
fn parses_create_or_replace_aggregate() {
    let stmt = create(
        "CREATE OR REPLACE AGGREGATE myavg (numeric)
         (
            stype = internal,
            sfunc = numeric_avg_accum,
            finalfunc = numeric_avg
         )",
    );
    assert!(
        stmt == CreateAggregateStmt {
            name: "myavg".into(),
            or_replace: true,
            args: Some(AggregateArgs::Args(vec![arg(builtin(
                ColumnType::Numeric(None)
            ))])),
            options: vec![
                AggregateOption::SType(named("internal")),
                AggregateOption::SFunc("numeric_avg_accum".into()),
                AggregateOption::FinalFunc("numeric_avg".into()),
            ],
        }
    );
}

#[test]
fn parses_an_empty_option_list() {
    // Which options an aggregate must carry is the executor's rule; the
    // grammar's job is only to read the list.
    assert!(
        create("CREATE AGGREGATE a (int4) ()")
            == CreateAggregateStmt {
                name: "a".into(),
                or_replace: false,
                args: Some(AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))])),
                options: Vec::new(),
            }
    );
}

// ---------------------------------------------------------------------------
// CREATE AGGREGATE — option values
// ---------------------------------------------------------------------------

#[test]
fn folds_the_numbered_option_spellings_onto_the_plain_ones() {
    let cases = [
        ("sfunc1 = int4pl", "sfunc = int4pl"),
        ("stype1 = int4", "stype = int4"),
        ("initcond1 = '0'", "initcond = '0'"),
    ];
    for (numbered, plain) in cases {
        let numbered = create(&format!("CREATE AGGREGATE a (int4) ({numbered})"));
        let plain = create(&format!("CREATE AGGREGATE a (int4) ({plain})"));
        assert!(numbered == plain);
    }
}

#[test]
fn option_names_are_case_insensitive() {
    // The lexer lowercases an unquoted word, so every casing of an option name
    // reaches the same arm. `aggregates.sql` writes them in upper case.
    let upper =
        create("CREATE AGGREGATE balk(int4) (SFUNC = int4pl, STYPE = int8, INITCOND = '0')");
    let lower =
        create("CREATE AGGREGATE balk(int4) (sfunc = int4pl, stype = int8, initcond = '0')");
    assert!(upper == lower);
}

#[test]
fn a_quoted_mixed_case_option_name_is_not_recognised() {
    // `create_aggregate.sql`'s `case_agg`: PostgreSQL compares the attribute
    // name case-sensitively, so a quoted `"Sfunc1"` is an attribute it does not
    // recognise rather than a spelling of `SFUNC1`. The parser carries the name
    // through as written for the executor to reject.
    assert!(
        options(
            "CREATE AGGREGATE case_agg (
                \"Sfunc1\" = int4pl,
                \"Basetype\" = int4,
                \"Stype1\" = int4,
                \"Initcond1\" = '0',
                \"Parallel\" = safe
             )"
        ) == vec![
            unimplemented("Sfunc1", "int4pl"),
            unimplemented("Basetype", "integer"),
            unimplemented("Stype1", "integer"),
            unimplemented("Initcond1", "0"),
            unimplemented("Parallel", "safe"),
        ]
    );
}

#[test]
fn parses_polymorphic_state_and_base_types() {
    assert!(
        options(
            "create aggregate least_agg(variadic items anyarray) (stype = anyelement, sfunc = least_accum)"
        ) == vec![
            AggregateOption::SType(named("anyelement")),
            AggregateOption::SFunc("least_accum".into()),
        ]
    );
    assert!(
        options(
            "CREATE AGGREGATE myaggp07a(BASETYPE = anyelement, SFUNC = tfnp, STYPE = int[],
              FINALFUNC = ffp, INITCOND = '{}')"
        ) == vec![
            AggregateOption::BaseType(Some(named("anyelement"))),
            AggregateOption::SFunc("tfnp".into()),
            AggregateOption::SType(builtin(ColumnType::Array(ElemType::Int4))),
            AggregateOption::FinalFunc("ffp".into()),
            AggregateOption::InitCond(Some("{}".into())),
        ]
    );
}

#[test]
fn keeps_the_variadic_mode_of_an_aggregate_argument() {
    assert!(
        create("create aggregate least_agg(variadic items anyarray) (stype = anyelement, sfunc = least_accum)")
            .args
            == Some(AggregateArgs::Args(vec![RoutineArg {
                name: Some("items".into()),
                mode: RoutineArgMode::Variadic,
                ty: named("anyarray"),
                default: None,
            }]))
    );
}

#[test]
fn records_the_options_this_engine_does_not_execute() {
    let cases = [
        ("sspace = 10000", unimplemented("sspace", "10000")),
        ("parallel = safe", unimplemented("parallel", "safe")),
        (
            "finalfunc_modify = read_write",
            unimplemented("finalfunc_modify", "read_write"),
        ),
        (
            "finalfunc_extra = true",
            unimplemented("finalfunc_extra", "true"),
        ),
        (
            "mstype = float8",
            unimplemented("mstype", "double precision"),
        ),
        ("msfunc = float8pl", unimplemented("msfunc", "float8pl")),
        ("minvfunc = float8mi", unimplemented("minvfunc", "float8mi")),
        ("minitcond = 'MI'", unimplemented("minitcond", "MI")),
        (
            "combinefunc = numeric_avg_combine",
            unimplemented("combinefunc", "numeric_avg_combine"),
        ),
        (
            "serialfunc = numeric_avg_serialize",
            unimplemented("serialfunc", "numeric_avg_serialize"),
        ),
        (
            "deserialfunc = numeric_avg_deserialize",
            unimplemented("deserialfunc", "numeric_avg_deserialize"),
        ),
        ("sortop = >", unimplemented("sortop", ">")),
    ];
    for (written, expected) in cases {
        let sql = format!("CREATE AGGREGATE a (int4) (sfunc = f, stype = int4, {written})");
        assert!(
            options(&sql)
                == vec![
                    AggregateOption::SFunc("f".into()),
                    AggregateOption::SType(builtin(ColumnType::Int4)),
                    expected,
                ]
        );
    }
}

#[test]
fn parses_the_bare_hypothetical_marker() {
    assert!(
        options("CREATE AGGREGATE a (int4) (sfunc = f, stype = internal, hypothetical)")
            == vec![
                AggregateOption::SFunc("f".into()),
                AggregateOption::SType(named("internal")),
                AggregateOption::Hypothetical,
            ]
    );
}

#[test]
fn reads_an_unquoted_initcond_number() {
    // `alter_generic.sql` writes `initcond = 0` and `initcond = -100`, and a
    // quoted `'0'` means the same thing: the state's value as external text.
    let cases = [
        ("initcond = 0", "0"),
        ("initcond = -100", "-100"),
        ("initcond = '0'", "0"),
        ("initcond = '{0,0}'", "{0,0}"),
    ];
    for (written, expected) in cases {
        let sql = format!(
            "CREATE AGGREGATE alt_agg2 (sfunc1 = int4mi, basetype = int4, stype1 = int4, {written})"
        );
        assert!(
            options(&sql).last() == Some(&AggregateOption::InitCond(Some(expected.to_string())))
        );
    }
    assert!(
        options("CREATE AGGREGATE a (int4) (sfunc = f, stype = int4, initcond = NULL)").last()
            == Some(&AggregateOption::InitCond(None))
    );
}

#[test]
fn drops_the_argument_list_written_after_a_function_valued_option() {
    // `aggregates.sql` writes `SFUNC = balkifnull(int8, int4)`. PostgreSQL
    // parses that as a type name with modifiers and keeps only the name, so the
    // written argument types name nothing.
    assert!(
        options(
            "CREATE AGGREGATE balk(int4)
             (
                 SFUNC = int4_sum(int8, int4),
                 STYPE = int8,
                 COMBINEFUNC = balkifnull(int8, int8),
                 PARALLEL = SAFE,
                 INITCOND = '0'
             )"
        ) == vec![
            AggregateOption::SFunc("int4_sum".into()),
            AggregateOption::SType(builtin(ColumnType::Int8)),
            unimplemented("combinefunc", "balkifnull"),
            unimplemented("parallel", "safe"),
            AggregateOption::InitCond(Some("0".into())),
        ]
    );
}

#[test]
fn a_trailing_comment_is_not_part_of_an_option_value() {
    assert!(
        options(
            "CREATE AGGREGATE myavg (numeric)
             (
                stype = numeric,
                sfunc = numeric_add,
                finalfunc_modify = shareable  -- just to test a non-default setting
             )"
        ) == vec![
            AggregateOption::SType(builtin(ColumnType::Numeric(None))),
            AggregateOption::SFunc("numeric_add".into()),
            unimplemented("finalfunc_modify", "shareable"),
        ]
    );
}

// ---------------------------------------------------------------------------
// DROP AGGREGATE
// ---------------------------------------------------------------------------

#[test]
fn parses_every_drop_aggregate_shape() {
    let cases = [
        (
            "DROP AGGREGATE myavg (numeric)",
            Statement::DropAggregate {
                if_exists: false,
                aggregates: vec![signature(
                    "myavg",
                    AggregateArgs::Args(vec![arg(builtin(ColumnType::Numeric(None)))]),
                )],
                cascade: false,
            },
        ),
        (
            "DROP AGGREGATE test_aggregate_exists(*)",
            Statement::DropAggregate {
                if_exists: false,
                aggregates: vec![signature("test_aggregate_exists", AggregateArgs::Star)],
                cascade: false,
            },
        ),
        (
            "DROP AGGREGATE IF EXISTS test_aggregate_exists(*)",
            Statement::DropAggregate {
                if_exists: true,
                aggregates: vec![signature("test_aggregate_exists", AggregateArgs::Star)],
                cascade: false,
            },
        ),
        (
            "DROP AGGREGATE IF EXISTS newcnt (int4) CASCADE",
            Statement::DropAggregate {
                if_exists: true,
                aggregates: vec![signature(
                    "newcnt",
                    AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))]),
                )],
                cascade: true,
            },
        ),
        (
            "DROP AGGREGATE newcnt (int4), newsum (int4) RESTRICT",
            Statement::DropAggregate {
                if_exists: false,
                aggregates: vec![
                    signature(
                        "newcnt",
                        AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))]),
                    ),
                    signature(
                        "newsum",
                        AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))]),
                    ),
                ],
                cascade: false,
            },
        ),
        (
            // `errors.sql` drops an aggregate on a type no catalog resolves;
            // the parser carries the name through for the executor to report.
            "drop aggregate newcnt (nonesuch)",
            Statement::DropAggregate {
                if_exists: false,
                aggregates: vec![signature(
                    "newcnt",
                    AggregateArgs::Args(vec![arg(named("nonesuch"))]),
                )],
                cascade: false,
            },
        ),
    ];
    for (sql, expected) in cases {
        assert!(one(sql) == expected);
    }
}

// ---------------------------------------------------------------------------
// ALTER AGGREGATE
// ---------------------------------------------------------------------------

#[test]
fn parses_every_alter_aggregate_action() {
    let int_sig = || {
        signature(
            "alt_agg1",
            AggregateArgs::Args(vec![arg(builtin(ColumnType::Int4))]),
        )
    };
    let cases = [
        (
            "ALTER AGGREGATE alt_agg1(int) RENAME TO alt_agg3",
            AlterRoutineAction::RenameTo("alt_agg3".into()),
        ),
        (
            "ALTER AGGREGATE alt_agg1(int) OWNER TO regress_alter_generic_user3",
            AlterRoutineAction::OwnerTo("regress_alter_generic_user3".into()),
        ),
        (
            "ALTER AGGREGATE alt_agg1(int) SET SCHEMA alt_nsp2",
            AlterRoutineAction::SetSchema("alt_nsp2".into()),
        ),
    ];
    for (sql, action) in cases {
        assert!(
            one(sql)
                == Statement::AlterAggregate {
                    aggregate: int_sig(),
                    action,
                }
        );
    }
}

#[test]
fn parses_alter_aggregate_on_the_star_form() {
    assert!(
        one("ALTER AGGREGATE newcnt(*) RENAME TO oldcnt")
            == Statement::AlterAggregate {
                aggregate: signature("newcnt", AggregateArgs::Star),
                action: AlterRoutineAction::RenameTo("oldcnt".into()),
            }
    );
}

// ---------------------------------------------------------------------------
// Ordered-set signatures
// ---------------------------------------------------------------------------

#[test]
fn parses_ordered_and_hypothetical_set_signatures() {
    let ordered = create(
        "CREATE AGGREGATE my_percentile_disc(float8 ORDER BY anyelement) (
           stype = internal, sfunc = ordered_set_transition,
           finalfunc = percentile_disc_final, finalfunc_extra = true)",
    );
    assert!(ordered.args == Some(AggregateArgs::Ordered {
        direct: vec![arg(builtin(ColumnType::Float8))],
        ordered: vec![arg(named("anyelement"))],
    }));
    assert!(ordered.options.contains(&AggregateOption::Unimplemented {
        name: "finalfunc_extra".into(),
        value: "true".into(),
    }));

    let hypothetical = create(
        "CREATE AGGREGATE my_rank(VARIADIC \"any\" ORDER BY VARIADIC \"any\") (
           stype = internal, sfunc = ordered_set_transition_multi, hypothetical)",
    );
    assert!(hypothetical.args == Some(AggregateArgs::Ordered {
        direct: vec![RoutineArg {
            name: None,
            mode: RoutineArgMode::Variadic,
            ty: named("any"),
            default: None,
        }],
        ordered: vec![RoutineArg {
            name: None,
            mode: RoutineArgMode::Variadic,
            ty: named("any"),
            default: None,
        }],
    }));
    assert!(hypothetical.options.contains(&AggregateOption::Hypothetical));
}

#[test]
fn parses_within_group_as_an_ordered_set_call() {
    let Expr::Func(call) = crabka_pgparser::parser::parse_expression(
        "n16_ordered(0.5) WITHIN GROUP (ORDER BY value DESC)",
    )
    .expect("parses")
    else {
        panic!("expected a function call");
    };
    assert!(call.name == "n16_ordered");
    assert!(call.within_group);
    assert!(call.order_by.len() == 1);
    assert!(!call.order_by[0].asc);
}

#[test]
fn refuses_a_malformed_statement_with_a_syntax_error() {
    let cases = [
        // An option pair with no `=`, no value, and no name.
        "CREATE AGGREGATE a (int4) (sfunc int4pl)",
        "CREATE AGGREGATE a (int4) (sfunc = )",
        "CREATE AGGREGATE a (int4) (= int4pl)",
        "CREATE AGGREGATE a (int4) (sfunc = f,)",
        // No definition list at all.
        "CREATE AGGREGATE a (int4)",
        // `errors.sql`: an aggregate is always named with its argument list.
        "drop aggregate",
        "drop aggregate newcnt1",
        "drop aggregate 314159 (int)",
        // ALTER AGGREGATE takes none of ALTER FUNCTION's definition options.
        "ALTER AGGREGATE a (int4) IMMUTABLE",
        "ALTER AGGREGATE a (int4)",
    ];
    for sql in cases {
        assert!(error(sql).sqlstate() == "42601", "{sql}");
    }
}

// ---------------------------------------------------------------------------
// Command identities, and the words this grammar must leave unreserved
// ---------------------------------------------------------------------------

#[test]
fn reports_the_command_identity_of_each_aggregate_statement() {
    let cases = [
        (
            "CREATE AGGREGATE a (int4) (sfunc = f, stype = int4)",
            CommandIdentity::CreateAggregate,
        ),
        (
            "CREATE OR REPLACE AGGREGATE a (int4) (sfunc = f, stype = int4)",
            CommandIdentity::CreateAggregate,
        ),
        (
            "CREATE AGGREGATE a (sfunc = f, basetype = int4, stype = int4)",
            CommandIdentity::CreateAggregate,
        ),
        ("DROP AGGREGATE a (int4)", CommandIdentity::DropAggregate),
        (
            "ALTER AGGREGATE a (int4) RENAME TO b",
            CommandIdentity::AlterAggregate,
        ),
    ];
    for (sql, expected) in cases {
        assert!(identity(sql) == expected, "{sql}");
    }
    assert!(CommandIdentity::CreateAggregate.name() == "CREATE AGGREGATE");
    assert!(CommandIdentity::DropAggregate.name() == "DROP AGGREGATE");
    assert!(CommandIdentity::AlterAggregate.name() == "ALTER AGGREGATE");
}

#[test]
fn aggregate_stays_an_unreserved_identifier() {
    // The whole grammar above is spelled with plain identifiers, so `aggregate`
    // is still available as a relation, a column and an alias.
    let cases = [
        (
            "CREATE TABLE aggregate (aggregate int)",
            CommandIdentity::CreateTable,
        ),
        ("SELECT aggregate FROM t", CommandIdentity::Select),
        (
            "SELECT aggregate.aggregate FROM aggregate",
            CommandIdentity::Select,
        ),
        ("INSERT INTO aggregate VALUES (1)", CommandIdentity::Insert),
        ("DROP TABLE aggregate", CommandIdentity::DropTable),
    ];
    for (sql, expected) in cases {
        assert!(identity(sql) == expected, "{sql}");
    }
}

#[test]
fn the_option_words_stay_unreserved_too() {
    let cases = [
        "SELECT sfunc, stype, basetype, initcond, finalfunc FROM t",
        "SELECT hypothetical, combinefunc, parallel, sspace FROM t",
        "CREATE TABLE t (sfunc int, stype int, basetype int, hypothetical int)",
    ];
    for sql in cases {
        assert!(parse(sql).is_ok(), "{sql}");
    }
}
