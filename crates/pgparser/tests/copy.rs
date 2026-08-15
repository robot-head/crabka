//! `COPY` grammar: both directions, both target forms, and both spellings of
//! the option list. Expectations are taken from `PostgreSQL` 18.4 — the accepted
//! forms and the exact error text it reports for the rejected ones.

use assert2::assert;
use crabka_pgparser::ast::{
    CopyColumns, CopyDestination, CopyDirection, CopyFormat, CopyHeader, CopyLogVerbosity,
    CopyOnError, CopyOptions, CopySource, CopyStmt, CopyTarget, Expr, QueryBody, QueryExpr,
    RelationRef, SelectItem, SetExpr, Statement,
};

/// Parse a statement that must be a single `COPY`.
fn copy(sql: &str) -> CopyStmt {
    let statements = crabka_pgparser::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    match <[Statement; 1]>::try_from(statements) {
        Ok([Statement::Copy(stmt)]) => *stmt,
        Ok([other]) => panic!("{sql}: expected a COPY statement, got {other:?}"),
        Err(other) => panic!("{sql}: expected one statement, got {other:?}"),
    }
}

/// The option list of a statement that parses, which is what most of the
/// grammar's surface actually varies.
fn options(sql: &str) -> CopyOptions {
    copy(sql).options
}

/// The `(sqlstate, message)` of a statement that must not parse.
fn rejection(sql: &str) -> (&'static str, String) {
    let error = crabka_pgparser::parse(sql).expect_err(sql);
    (error.sqlstate(), error.message.clone())
}

fn table(name: &str) -> CopyTarget {
    CopyTarget::Table {
        name: RelationRef::bare(name),
        columns: None,
    }
}

#[test]
fn direction_and_endpoint_pair_up() {
    let cases = [
        ("COPY t FROM STDIN", CopyDirection::From(CopySource::Stdin)),
        (
            "COPY t FROM '/tmp/in.tsv'",
            CopyDirection::From(CopySource::File("/tmp/in.tsv".into())),
        ),
        (
            "COPY t TO STDOUT",
            CopyDirection::To(CopyDestination::Stdout),
        ),
        (
            "COPY t TO '/tmp/out.tsv'",
            CopyDirection::To(CopyDestination::File("/tmp/out.tsv".into())),
        ),
        // `copy_file_name` yields "no file" for STDIN and STDOUT alike and the
        // direction decides which stream that is, so PostgreSQL silently
        // corrects the mismatched spelling rather than rejecting it.
        (
            "COPY t TO STDIN",
            CopyDirection::To(CopyDestination::Stdout),
        ),
        ("COPY t FROM STDOUT", CopyDirection::From(CopySource::Stdin)),
    ];
    for (sql, direction) in cases {
        assert!(
            copy(sql)
                == CopyStmt {
                    target: table("t"),
                    direction,
                    options: CopyOptions::default(),
                },
            "{sql}"
        );
    }
}

#[test]
fn a_column_list_restricts_the_table_form() {
    assert!(
        copy("COPY s1.accounts (id, name) TO STDOUT")
            == CopyStmt {
                target: CopyTarget::Table {
                    name: RelationRef::qualified("s1", "accounts"),
                    columns: Some(vec!["id".into(), "name".into()]),
                },
                direction: CopyDirection::To(CopyDestination::Stdout),
                options: CopyOptions::default(),
            }
    );
}

#[test]
fn the_query_form_carries_the_parsed_query() {
    let stmt = copy("COPY (SELECT 1) TO STDOUT");
    let CopyTarget::Query(query) = &stmt.target else {
        panic!("expected a query target, got {:?}", stmt.target);
    };
    let Statement::Query(QueryExpr {
        body: SetExpr::Query(QueryBody::Select(select)),
        ..
    }) = &**query
    else {
        panic!("expected a SELECT, got {query:?}");
    };
    assert!(
        select.projection
            == vec![SelectItem::Expr {
                expr: Expr::IntLiteral("1".into()),
                alias: None,
            }]
    );
    assert!(stmt.direction == CopyDirection::To(CopyDestination::Stdout));
}

/// Every query body `PreparableStmt` allows, including the data-modifying ones
/// that hand their rows back through `RETURNING`.
#[test]
fn the_query_form_accepts_every_preparable_statement() {
    for sql in [
        "COPY (SELECT t FROM test1 WHERE id = 1) TO STDOUT",
        "COPY (SELECT t FROM test1 WHERE id = 3 FOR UPDATE) TO STDOUT",
        "COPY (SELECT * FROM test1 JOIN test2 USING (id)) TO STDOUT",
        "COPY (SELECT 1 UNION SELECT 2 ORDER BY 1) TO STDOUT",
        "COPY (VALUES (1), (2)) TO STDOUT",
        "COPY (TABLE test1) TO STDOUT",
        "COPY (WITH x AS (SELECT 1) SELECT * FROM x) TO STDOUT",
        "COPY (INSERT INTO copydml_test (t) VALUES ('f') RETURNING id) TO STDOUT",
        "COPY (UPDATE copydml_test SET t = 'g' WHERE t = 'f' RETURNING id) TO STDOUT",
        "COPY (DELETE FROM copydml_test WHERE t = 'g' RETURNING id) TO STDOUT",
        "COPY (WITH x AS (SELECT 1 AS id) DELETE FROM copydml_test RETURNING id) TO STDOUT",
    ] {
        let stmt = copy(sql);
        assert!(matches!(stmt.target, CopyTarget::Query(_)), "{sql}");
        assert!(
            stmt.direction == CopyDirection::To(CopyDestination::Stdout),
            "{sql}"
        );
    }
}

/// The modern parenthesized list and the legacy bare-keyword tail are two
/// spellings of one option set, so equivalent statements must fold to the same
/// [`CopyOptions`].
#[test]
fn the_two_option_spellings_agree() {
    let cases = [
        ("COPY t TO STDOUT (FORMAT csv)", "COPY t TO STDOUT WITH CSV"),
        (
            "COPY t TO STDOUT (FORMAT csv, HEADER)",
            "COPY t TO STDOUT CSV HEADER",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, QUOTE '''', DELIMITER '|')",
            "COPY t TO STDOUT WITH CSV QUOTE AS '''' DELIMITER AS '|'",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE (col2), ESCAPE '\\')",
            "COPY t TO STDOUT WITH CSV FORCE QUOTE col2 ESCAPE '\\'",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE *)",
            "COPY t TO STDOUT WITH CSV FORCE QUOTE *",
        ),
        (
            "COPY t FROM STDIN (FORMAT csv, FORCE_NOT_NULL (a, b))",
            "COPY t FROM STDIN WITH CSV FORCE NOT NULL a, b",
        ),
        (
            "COPY t FROM STDIN (FORMAT csv, FORCE_NULL *)",
            "COPY t FROM STDIN WITH CSV FORCE NULL *",
        ),
        (
            "COPY t TO STDOUT (NULL 'I''m null', ENCODING 'sql_ascii')",
            "COPY t TO STDOUT WITH NULL AS 'I''m null' ENCODING 'sql_ascii'",
        ),
        // `[USING] DELIMITERS 'c'` is the pre-`WITH` spelling of DELIMITER.
        (
            "COPY t TO STDOUT (DELIMITER '|')",
            "COPY t TO STDOUT USING DELIMITERS '|'",
        ),
        (
            "COPY t TO STDOUT (DELIMITER '|')",
            "COPY t TO STDOUT DELIMITERS '|'",
        ),
        // `WITH` is optional before the parenthesized list, and an empty legacy
        // list is legal with or without it.
        ("COPY t TO STDOUT WITH (FORMAT csv)", "COPY t TO STDOUT CSV"),
        ("COPY t TO STDOUT", "COPY t TO STDOUT WITH"),
    ];
    for (parenthesized, legacy) in cases {
        assert!(
            options(parenthesized) == options(legacy),
            "{parenthesized} vs {legacy}"
        );
    }
}

#[test]
fn option_values_land_in_typed_fields() {
    let stdout_csv = CopyOptions {
        format: CopyFormat::Csv,
        ..CopyOptions::default()
    };
    let cases: &[(&str, CopyOptions)] = &[
        (
            "COPY t TO STDOUT (FORMAT csv, HEADER, DELIMITER '|', NULL '')",
            CopyOptions {
                header: Some(CopyHeader::True),
                delimiter: Some("|".into()),
                null: Some(String::new()),
                ..stdout_csv.clone()
            },
        ),
        // A boolean option takes 0/1 as well as the four words, in any case.
        (
            "COPY t TO STDOUT (HEADER 0)",
            CopyOptions {
                header: Some(CopyHeader::False),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t TO STDOUT (HEADER 'TRUE')",
            CopyOptions {
                header: Some(CopyHeader::True),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t TO STDOUT (HEADER off)",
            CopyOptions {
                header: Some(CopyHeader::False),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t FROM STDIN (HEADER match)",
            CopyOptions {
                header: Some(CopyHeader::Match),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE (a, b))",
            CopyOptions {
                force_quote: Some(CopyColumns::Named(vec!["a".into(), "b".into()])),
                ..stdout_csv.clone()
            },
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE *)",
            CopyOptions {
                force_quote: Some(CopyColumns::All),
                ..stdout_csv
            },
        ),
        (
            "COPY t FROM STDIN (FREEZE, DEFAULT '\\D', ON_ERROR ignore, REJECT_LIMIT 3)",
            CopyOptions {
                freeze: true,
                default: Some("\\D".into()),
                on_error: Some(CopyOnError::Ignore),
                reject_limit: Some(3),
                ..CopyOptions::default()
            },
        ),
        // The undocumented binary-conversion filter, which alone among the
        // column-list options may be written bare.
        (
            "COPY t FROM STDIN (CONVERT_SELECTIVELY (a))",
            CopyOptions {
                convert_selectively: Some(vec!["a".into()]),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t FROM STDIN (CONVERT_SELECTIVELY)",
            CopyOptions {
                convert_selectively: Some(Vec::new()),
                ..CopyOptions::default()
            },
        ),
        (
            "COPY t FROM STDIN (FREEZE off, ON_ERROR stop, LOG_VERBOSITY 'VERBOSE')",
            CopyOptions {
                freeze: false,
                on_error: Some(CopyOnError::Stop),
                log_verbosity: Some(CopyLogVerbosity::Verbose),
                ..CopyOptions::default()
            },
        ),
    ];
    for (sql, expected) in cases {
        assert!(options(sql) == *expected, "{sql}");
    }
}

/// Every rejection `PostgreSQL` reports from the statement alone, with its text.
#[test]
fn rejected_forms_report_postgresqls_own_errors() {
    let cases: &[(&str, &str, &str)] = &[
        // Grammar.
        (
            "COPY (SELECT 1) FROM STDIN",
            "42601",
            "syntax error at or near \"FROM\"",
        ),
        (
            "COPY (SELECT 1) (a) TO STDOUT",
            "42601",
            "syntax error at or near \"(\"",
        ),
        (
            "COPY t TO STDOUT WHERE a = 1",
            "42601",
            "WHERE clause not allowed with COPY TO",
        ),
        (
            "COPY t TO STDOUT WITH OIDS",
            "42601",
            "syntax error at or near \"OIDS\"",
        ),
        (
            "COPY t TO STDOUT WITH DELIMITERS '|'",
            "42601",
            "syntax error at or near \"DELIMITERS\"",
        ),
        // Option names and values.
        (
            "COPY t TO STDOUT (bogus)",
            "42601",
            "option \"bogus\" not recognized",
        ),
        // ColLabel keeps a quoted option name's case, and the match is
        // case-sensitive, so the quoted spelling is a different option.
        (
            "COPY t TO STDOUT (\"FORMAT\" csv)",
            "42601",
            "option \"FORMAT\" not recognized",
        ),
        (
            "COPY t TO STDOUT (FORMAT)",
            "42601",
            "format requires a parameter",
        ),
        (
            "COPY t TO STDOUT (DELIMITER)",
            "42601",
            "delimiter requires a parameter",
        ),
        (
            "COPY t TO STDOUT (FORMAT default)",
            "22023",
            "COPY format \"default\" not recognized",
        ),
        // A format name is matched case-sensitively too, so the quoted spelling
        // never folds down to one.
        (
            "COPY t TO STDOUT (FORMAT 'CSV')",
            "22023",
            "COPY format \"CSV\" not recognized",
        ),
        (
            "COPY t TO STDOUT (HEADER maybe)",
            "42601",
            "header requires a Boolean value or \"match\"",
        ),
        (
            "COPY t FROM STDIN (FREEZE maybe)",
            "42601",
            "freeze requires a Boolean value",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE a)",
            "22023",
            "argument to option \"force_quote\" must be a list of column names",
        ),
        (
            "COPY t FROM STDIN (ON_ERROR unsupported)",
            "22023",
            "COPY ON_ERROR \"unsupported\" not recognized",
        ),
        (
            "COPY t FROM STDIN (LOG_VERBOSITY loud)",
            "22023",
            "COPY LOG_VERBOSITY \"loud\" not recognized",
        ),
        (
            "COPY t FROM STDIN (REJECT_LIMIT 'x')",
            "22P02",
            "invalid input syntax for type bigint: \"x\"",
        ),
        (
            "COPY t FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 0)",
            "22023",
            "REJECT_LIMIT (0) must be greater than zero",
        ),
        (
            "COPY t FROM STDIN (REJECT_LIMIT 1)",
            "22023",
            "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
        ),
        (
            "COPY t FROM STDIN (CONVERT_SELECTIVELY (a), CONVERT_SELECTIVELY (b))",
            "42601",
            "conflicting or redundant options",
        ),
        (
            "COPY t FROM STDIN (ENCODING 'sql_ascii', ENCODING 'sql_ascii')",
            "42601",
            "conflicting or redundant options",
        ),
        (
            "COPY t TO STDOUT WITH CSV CSV",
            "42601",
            "conflicting or redundant options",
        ),
        // Options on the wrong side of the copy, or outside CSV mode.
        (
            "COPY t TO STDOUT (HEADER match)",
            "0A000",
            "cannot use \"match\" with HEADER in COPY TO",
        ),
        (
            "COPY t TO STDOUT (ON_ERROR stop)",
            "22023",
            "COPY ON_ERROR cannot be used with COPY TO",
        ),
        (
            "COPY t TO STDOUT (FREEZE)",
            "22023",
            "COPY FREEZE cannot be used with COPY TO",
        ),
        (
            "COPY (SELECT 1 AS test) TO STDOUT WITH (DEFAULT '\\D')",
            "0A000",
            "COPY DEFAULT cannot be used with COPY TO",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_NOT_NULL (a))",
            "22023",
            "COPY FORCE_NOT_NULL cannot be used with COPY TO",
        ),
        (
            "COPY t TO STDOUT (FORMAT csv, FORCE_NULL *)",
            "22023",
            "COPY FORCE_NULL cannot be used with COPY TO",
        ),
        (
            "COPY t FROM STDIN (FORMAT csv, FORCE_QUOTE *)",
            "0A000",
            "COPY FORCE_QUOTE cannot be used with COPY FROM",
        ),
        (
            "COPY t TO STDOUT (FORCE_QUOTE *)",
            "0A000",
            "COPY FORCE_QUOTE requires CSV mode",
        ),
        (
            "COPY t TO STDOUT (QUOTE '''')",
            "0A000",
            "COPY QUOTE requires CSV mode",
        ),
        (
            "COPY t TO STDOUT (ESCAPE '\\')",
            "0A000",
            "COPY ESCAPE requires CSV mode",
        ),
        // The query form's own refusals.
        (
            "COPY (SELECT t INTO temp test3 FROM test1) TO STDOUT",
            "0A000",
            "COPY (SELECT INTO) is not supported",
        ),
        (
            "COPY (INSERT INTO copydml_test DEFAULT VALUES) TO STDOUT",
            "0A000",
            "COPY query must have a RETURNING clause",
        ),
        (
            "COPY (UPDATE copydml_test SET t = 'g') TO STDOUT",
            "0A000",
            "COPY query must have a RETURNING clause",
        ),
        (
            "COPY (DELETE FROM copydml_test) TO STDOUT",
            "0A000",
            "COPY query must have a RETURNING clause",
        ),
    ];
    for (sql, sqlstate, message) in cases {
        assert!(
            rejection(sql) == (*sqlstate, (*message).to_string()),
            "{sql}"
        );
    }
}

/// Grammar `PostgreSQL` accepts that this engine refuses outright, each with the
/// `feature_not_supported` code that says so.
#[test]
fn unsupported_grammar_is_feature_not_supported() {
    let cases: &[(&str, &str)] = &[
        (
            "COPY t TO STDOUT (FORMAT binary)",
            "COPY BINARY is not supported",
        ),
        (
            "COPY t TO STDOUT WITH BINARY",
            "COPY BINARY is not supported",
        ),
        ("COPY BINARY t TO STDOUT", "COPY BINARY is not supported"),
        (
            "COPY t TO PROGRAM 'cat'",
            "COPY TO PROGRAM is not supported",
        ),
        (
            "COPY t FROM PROGRAM 'echo 1'",
            "COPY FROM PROGRAM is not supported",
        ),
        (
            "COPY t FROM STDIN WHERE a = 1",
            "WHERE clause in COPY FROM is not supported",
        ),
    ];
    for (sql, message) in cases {
        assert!(rejection(sql) == ("0A000", (*message).to_string()), "{sql}");
    }
}
